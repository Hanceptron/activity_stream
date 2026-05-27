"""StreamGuard input capture agent.

Captures keyboard and mouse events on macOS via pynput and emits one
JSON object per event to stdout or Kafka. See README for the macOS
permissions this needs and why it cannot run in a container.

Sleep/wake recovery: on macOS the kernel kills pynput's event tap
during sleep, so the listeners look alive but never fire after wake.
The agent subscribes to NSWorkspaceDidWakeNotification and re-creates
the listeners every time the system wakes; NSWorkspaceWillSleepNotification
triggers a Kafka flush so in-flight events drain before the socket dies.
The main loop spins the NSRunLoop so the notification center can
dispatch — `time.sleep` alone would never see them. On non-macOS
platforms the observer falls back to a plain sleep and recovery is
limited to the outer `while true` restart loop.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time

from pynput import keyboard, mouse

USER_ID = "user-001"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events.raw"

# Cap mouse-move emission at ~50/sec. Clicks and scrolls are never throttled.
MOVE_MIN_INTERVAL = 0.020

# pyobjc is only available (and only meaningful) on macOS. Importing
# inside a try/except lets the agent still run on Linux for testing,
# where wake recovery is a no-op.
try:
    import objc
    from AppKit import NSWorkspace
    from Foundation import NSDate, NSObject, NSRunLoop
    _HAS_PYOBJC = True
except ImportError:
    _HAS_PYOBJC = False

log = logging.getLogger("streamguard.agent")


def _key_id(key) -> str:
    # Stable id only. Printable keys → the character ("a", "A", "1"),
    # special keys → pynput's "Key.space"-style repr. Downstream uses
    # this for timing / n-gram features and MUST NOT persist raw text.
    if isinstance(key, keyboard.KeyCode) and key.char is not None:
        return key.char
    return str(key)


def _stdout_sink(event: dict) -> None:
    sys.stdout.write(json.dumps(event) + "\n")
    sys.stdout.flush()


def _on_kafka_error(err) -> None:
    # confluent-kafka calls this from its background poll thread when
    # the broker becomes unreachable or returns a delivery error.
    # Without it, post-sleep socket failures vanish silently.
    log.warning("kafka producer error: %s", err)


def _kafka_sink():
    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "streamguard-agent",
        # Reconnect with exponential backoff so the socket recovers
        # on its own after sleep/wake or a docker restart.
        "reconnect.backoff.ms": 500,
        "reconnect.backoff.max.ms": 10_000,
        "socket.keepalive.enable": True,
        # Retry transient send failures; capped low because input
        # events are cheap to lose if the broker really is down.
        "message.send.max.retries": 5,
        "retry.backoff.ms": 200,
        "error_cb": _on_kafka_error,
    })
    user_key = USER_ID.encode()

    def send(event: dict) -> None:
        producer.poll(0)
        producer.produce(KAFKA_TOPIC, key=user_key, value=json.dumps(event).encode())

    def flush() -> None:
        producer.flush(5)

    return send, flush


def _build_listeners(send):
    last_move = 0.0

    def on_press(key):
        send({"type": "key_down", "key": _key_id(key), "user": USER_ID, "ts": time.time()})

    def on_release(key):
        send({"type": "key_up", "key": _key_id(key), "user": USER_ID, "ts": time.time()})

    def on_move(x, y):
        nonlocal last_move
        t = time.time()
        if t - last_move < MOVE_MIN_INTERVAL:
            return
        last_move = t
        send({"type": "move", "x": int(x), "y": int(y), "user": USER_ID, "ts": t})

    def on_click(x, y, button, pressed):
        send({
            "type": "click", "x": int(x), "y": int(y),
            "button": str(button), "pressed": bool(pressed),
            "user": USER_ID, "ts": time.time(),
        })

    def on_scroll(x, y, dx, dy):
        send({
            "type": "scroll", "x": int(x), "y": int(y),
            "dx": int(dx), "dy": int(dy),
            "user": USER_ID, "ts": time.time(),
        })

    kb = keyboard.Listener(on_press=on_press, on_release=on_release)
    ms = mouse.Listener(on_move=on_move, on_click=on_click, on_scroll=on_scroll)
    return kb, ms


class _ListenerSupervisor:
    # Owns the active pynput listeners and re-creates them on demand.
    # The lock guards the swap so a wake-notification handler firing
    # while the main loop is checking is_alive() can't observe a
    # half-set state.
    def __init__(self, send):
        self._send = send
        self._lock = threading.Lock()
        self._kb = None
        self._ms = None

    def start(self):
        with self._lock:
            self._kb, self._ms = _build_listeners(self._send)
            self._kb.start()
            self._ms.start()

    def restart(self):
        with self._lock:
            for listener in (self._kb, self._ms):
                if listener is None:
                    continue
                try:
                    listener.stop()
                except Exception:
                    pass
            self._kb, self._ms = _build_listeners(self._send)
            self._kb.start()
            self._ms.start()
        log.info("agent listeners restarted after wake at %s", time.time())

    def stop(self):
        with self._lock:
            for listener in (self._kb, self._ms):
                if listener is not None:
                    try:
                        listener.stop()
                    except Exception:
                        pass

    def is_alive(self) -> bool:
        with self._lock:
            return (
                self._kb is not None
                and self._ms is not None
                and self._kb.is_alive()
                and self._ms.is_alive()
            )


if _HAS_PYOBJC:
    class _WakeObserver(NSObject):
        # NSObject subclass so NSNotificationCenter can call the
        # `workspace…:` selectors. pyobjc bridges trailing-underscore
        # method names to ObjC selectors with trailing colons, so
        # `workspaceDidWake_` becomes `workspaceDidWake:`.
        def initWithCallbacks_(self, callbacks):
            self = objc.super(_WakeObserver, self).init()
            if self is None:
                return None
            self._on_wake, self._on_sleep = callbacks
            return self

        def workspaceWillSleep_(self, _notification):
            try:
                self._on_sleep()
            except Exception:
                log.exception("error in sleep handler")

        def workspaceDidWake_(self, _notification):
            try:
                self._on_wake()
            except Exception:
                log.exception("error in wake handler")


def _install_wake_observer(on_wake, on_sleep):
    # Returns (observer, pump): the observer must outlive the run
    # loop or NSNotificationCenter will drop it; `pump(seconds)`
    # blocks the caller while spinning the NSRunLoop so any pending
    # sleep/wake notifications can fire. Falls back to time.sleep on
    # non-macOS systems where pyobjc isn't available.
    if not _HAS_PYOBJC:
        log.warning(
            "pyobjc not available; wake-recovery disabled. "
            "Install pyobjc-framework-Cocoa on macOS."
        )
        return None, time.sleep

    observer = _WakeObserver.alloc().initWithCallbacks_((on_wake, on_sleep))
    nc = NSWorkspace.sharedWorkspace().notificationCenter()
    nc.addObserver_selector_name_object_(
        observer, "workspaceDidWake:", "NSWorkspaceDidWakeNotification", None
    )
    nc.addObserver_selector_name_object_(
        observer, "workspaceWillSleep:", "NSWorkspaceWillSleepNotification", None
    )

    def pump(seconds: float) -> None:
        NSRunLoop.currentRunLoop().runUntilDate_(
            NSDate.dateWithTimeIntervalSinceNow_(seconds)
        )

    return observer, pump


def main() -> None:
    # Logging goes to stderr so it does not corrupt the JSON event
    # stream on stdout when --sink stdout is used.
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="StreamGuard input capture agent")
    parser.add_argument("--sink", choices=("stdout", "kafka"), default="stdout")
    args = parser.parse_args()

    flush = None
    if args.sink == "kafka":
        send, flush = _kafka_sink()
    else:
        send = _stdout_sink

    supervisor = _ListenerSupervisor(send)
    supervisor.start()

    def on_sleep() -> None:
        if flush is not None:
            try:
                flush()
            except Exception:
                log.exception("kafka flush before sleep failed")

    def on_wake() -> None:
        supervisor.restart()

    _observer, pump = _install_wake_observer(on_wake, on_sleep)

    try:
        while supervisor.is_alive():
            pump(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        supervisor.stop()
        if flush is not None:
            flush()


if __name__ == "__main__":
    main()

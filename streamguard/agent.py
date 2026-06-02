"""StreamGuard input capture agent.

Captures keyboard and mouse events on macOS via pynput and emits one
JSON object per event to stdout or Kafka. See README for the macOS
permissions this needs and why it cannot run in a container.

Sleep/wake recovery: on macOS the kernel kills pynput's event tap
during sleep. In-process re-creation of pynput listeners after wake
is unreliable - the new Listener threads report is_alive() True but
the CGEventTap can be silently dead, so the agent looks healthy
while producing nothing. Instead, on
NSWorkspaceDidWakeNotification the agent flags itself for exit; the
outer `while true` restart loop in startup-tmux.sh respawns a fresh
Python process whose CGEventTap is freshly installed.
NSWorkspaceWillSleepNotification triggers a Kafka flush so in-flight
events drain before the socket dies. A second guard is the
HID-idle watchdog: if macOS reports the user as actively interacting
but the agent has not produced an event in WATCHDOG_SILENT_SECONDS,
the listener is presumed dead and the same exit/respawn path runs.
The main loop spins the NSRunLoop so the notification center can
dispatch - `time.sleep` alone would never see them. On non-macOS
platforms the observer and the watchdog both no-op; recovery falls
back to the outer restart loop.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import threading
import time
from pathlib import Path

from pynput import keyboard, mouse

USER_ID = "user-001"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events.raw"

# Cap mouse-move emission at ~50/sec. Clicks and scrolls are never throttled.
MOVE_MIN_INTERVAL = 0.020

# Watchdog: if the OS reports the user has interacted with input devices
# within this many seconds but the agent has not emitted an event for at
# least WATCHDOG_SILENT_SECONDS, the listener is presumed dead and we
# request exit so the outer restart loop can respawn a fresh process.
WATCHDOG_ACTIVE_SECONDS = 5.0
WATCHDOG_SILENT_SECONDS = 30.0
WATCHDOG_CHECK_INTERVAL = 5.0

# pyobjc is only available (and only meaningful) on macOS. Importing
# inside a try/except lets the agent still run on Linux for testing,
# where wake recovery is a no-op.
try:
    import objc
    from AppKit import NSScreen, NSWorkspace
    from ApplicationServices import AXIsProcessTrusted
    from Foundation import NSDate, NSObject, NSRunLoop
    _HAS_PYOBJC = True
except ImportError:
    _HAS_PYOBJC = False
    AXIsProcessTrusted = None  # type: ignore
    NSScreen = None  # type: ignore

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
    # Owns the active pynput listeners for the lifetime of the
    # process. We deliberately do NOT support in-process restart -
    # re-creating Listener objects on the fly after a macOS sleep
    # leaves the CGEventTap in a quietly broken state. Wake recovery
    # exits the whole process instead, so the outer bash restart loop
    # respawns a clean Python interpreter.
    def __init__(self, send):
        self._send = send
        self._kb = None
        self._ms = None

    def start(self):
        self._kb, self._ms = _build_listeners(self._send)
        self._kb.start()
        self._ms.start()

    def stop(self):
        for listener in (self._kb, self._ms):
            if listener is not None:
                try:
                    listener.stop()
                except Exception:
                    pass

    def is_alive(self) -> bool:
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


def _hid_idle_seconds() -> float | None:
    # Read HIDIdleTime out of IOHIDSystem via `ioreg`. Returns
    # seconds since the last HID (keyboard/mouse/trackpad) event, or
    # None on any parse failure. This is the same value macOS uses
    # internally for the "user is idle" state, so it tells us whether
    # the user is actually interacting with the machine regardless of
    # whether our pynput listener noticed.
    try:
        out = subprocess.check_output(
            ["ioreg", "-c", "IOHIDSystem", "-rd1"],
            text=True,
            timeout=2.0,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.SubprocessError, OSError):
        return None
    for line in out.splitlines():
        if "HIDIdleTime" not in line:
            continue
        try:
            ns = int(line.split("=", 1)[1].strip())
        except (IndexError, ValueError):
            return None
        return ns / 1_000_000_000.0
    return None


def _record_display_size() -> None:
    # Write the primary display's size (in points) to
    # output/display.json so the dashboard can frame the mouse heatmap
    # to the real screen and clip any external-monitor tail. The
    # magnitude of NSScreen.mainScreen().frame().size matches pynput's
    # mouse coordinate range, which is what the heatmap is built from.
    # Rewritten on every startup (and the agent restarts on wake), so
    # the value follows the current monitor setup. Screen queries do
    # not require Accessibility, so this works even when input capture
    # is not yet permitted. Best-effort: any failure is non-fatal.
    if not _HAS_PYOBJC or NSScreen is None:
        return
    try:
        size = NSScreen.mainScreen().frame().size
        Path("output").mkdir(exist_ok=True)
        Path("output/display.json").write_text(
            json.dumps({"width": int(size.width), "height": int(size.height)})
        )
        log.info("recorded display size %dx%d", int(size.width), int(size.height))
    except Exception:
        log.warning("could not record display size", exc_info=True)


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

    _record_display_size()

    flush = None
    if args.sink == "kafka":
        raw_send, flush = _kafka_sink()
    else:
        raw_send = _stdout_sink

    # Heartbeat for the watchdog. Each event we successfully hand to
    # the sink resets this to wall-clock now. The watchdog uses it to
    # tell "agent is silent because user is idle" (fine) apart from
    # "agent is silent because the event tap is dead" (force respawn).
    # `events_seen` separately tracks whether ANY event has ever been
    # observed - the watchdog refuses to fire until at least one event
    # has been captured, which is the most reliable way to tell
    # "pynput is working" from "pynput never had permission". This
    # works under both Accessibility and Input Monitoring TCC grants
    # without distinguishing between them.
    last_event_ts = time.time()
    events_seen = False

    def send(event: dict) -> None:
        nonlocal last_event_ts, events_seen
        last_event_ts = time.time()
        events_seen = True
        raw_send(event)

    # Warn loudly when the OS reports no Accessibility permission.
    # This is not the gate for the watchdog (Input Monitoring alone is
    # usually enough for pynput's mouse tap, and AXIsProcessTrusted only
    # checks the Accessibility entry), but it is the most common reason
    # for the agent to sit silent, so we surface it in the tmux pane.
    if AXIsProcessTrusted is not None and not bool(AXIsProcessTrusted()):
        log.warning(
            "macOS Accessibility not granted to this process - pynput "
            "may not capture keyboard events. Grant Accessibility (and "
            "Input Monitoring) to whichever terminal launched this "
            "agent, then fully quit and relaunch the terminal."
        )

    supervisor = _ListenerSupervisor(send)
    supervisor.start()
    log.info(
        "agent listening (sink=%s user=%s) - waiting for input events",
        args.sink,
        USER_ID,
    )

    # Cross-thread "exit cleanly" flag. Set from the wake handler
    # (which runs on the main thread inside the NSRunLoop pump) and
    # from the watchdog logic below. The main loop tests it every
    # pump cycle and breaks out, letting the finally block flush.
    exit_requested = threading.Event()

    def on_sleep() -> None:
        log.info("sleep notification received; flushing kafka before the socket dies")
        if flush is not None:
            try:
                flush()
            except Exception:
                log.exception("kafka flush before sleep failed")

    def on_wake() -> None:
        log.info("wake notification received; exiting for fresh respawn")
        exit_requested.set()

    _observer, pump = _install_wake_observer(on_wake, on_sleep)

    last_watchdog_check = time.time()

    try:
        while supervisor.is_alive() and not exit_requested.is_set():
            pump(0.2)

            now = time.time()
            if now - last_watchdog_check < WATCHDOG_CHECK_INTERVAL:
                continue
            last_watchdog_check = now

            if not events_seen:
                # Never observed an event - we have no proof pynput is
                # actually working, so the watchdog would just crash-loop
                # on the no-permission state. Stay quiet.
                continue
            idle = _hid_idle_seconds()
            if idle is None:
                continue
            silent_for = now - last_event_ts
            if idle < WATCHDOG_ACTIVE_SECONDS and silent_for > WATCHDOG_SILENT_SECONDS:
                log.error(
                    "watchdog: HID idle=%.1fs but no events for %.1fs - "
                    "listener presumed dead, exiting for fresh respawn",
                    idle,
                    silent_for,
                )
                exit_requested.set()
    except KeyboardInterrupt:
        pass
    finally:
        supervisor.stop()
        if flush is not None:
            flush()


if __name__ == "__main__":
    main()

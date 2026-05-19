"""StreamGuard input capture agent.

Captures keyboard and mouse events on macOS via pynput and emits one
JSON object per event to stdout or Kafka. See README for the macOS
permissions this needs and why it cannot run in a container.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from pynput import keyboard, mouse

USER_ID = "user-001"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events.raw"

# Cap mouse-move emission at ~50/sec. Clicks and scrolls are never throttled.
MOVE_MIN_INTERVAL = 0.020


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


def _kafka_sink():
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
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


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamGuard input capture agent")
    parser.add_argument("--sink", choices=("stdout", "kafka"), default="stdout")
    args = parser.parse_args()

    flush = None
    if args.sink == "kafka":
        send, flush = _kafka_sink()
    else:
        send = _stdout_sink

    kb, ms = _build_listeners(send)
    kb.start()
    ms.start()
    try:
        while kb.is_alive() and ms.is_alive():
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        kb.stop()
        ms.stop()
        if flush is not None:
            flush()


if __name__ == "__main__":
    main()

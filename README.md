# streamguard

Real-time insider-threat detection demo built on keyboard + mouse
behavioral biometrics. This repo currently contains only the input
capture agent; the consumer, feature extractor, model, and dashboard
will live alongside it in the `streamguard/` package.

## Native macOS only — do not containerize

The agent reads from the host's HID stream via the macOS Quartz event
tap (`pynput`). A Docker container has no access to host input devices,
so the agent must run as a native process on the Mac being monitored.

## macOS permissions

Both of these toggles are required for the terminal app that launches
Python (Terminal.app, iTerm2, VS Code's integrated terminal, etc.):

- **System Settings → Privacy & Security → Accessibility**
- **System Settings → Privacy & Security → Input Monitoring**

Keyboard capture specifically depends on **Input Monitoring**; without
it, mouse events still flow but key events do not.

After toggling either permission, **fully quit and relaunch** the
terminal app (Cmd+Q, not just close the window). macOS caches the
permission state at process start and will not pick up changes until
the parent process is restarted.

## Setup

```sh
uv sync
```

This installs `pynput` and `confluent-kafka` into `.venv/`. Python 3.12.

## Run

```sh
# stdout: one JSON event per line — use this to verify capture works
uv run python -m streamguard.agent

# kafka: produce to topic `events.raw` on localhost:9092, keyed by user
uv run python -m streamguard.agent --sink kafka
```

Stop with `Ctrl+C`. The agent stops both listeners and, in Kafka mode,
flushes the producer before exiting.

## Recording an enrollment session

The stdout sink is the simplest way to capture a labeled session for
training before any Kafka infrastructure exists:

```sh
uv run python -m streamguard.agent > session.jsonl
```

Type and use the mouse normally for a few minutes, then `Ctrl+C`. Each
line of `session.jsonl` is one event.

## Event shapes

All events carry `user` and `ts` (epoch seconds, float).

| `type`              | extra fields                                  |
| ------------------- | --------------------------------------------- |
| `key_down`/`key_up` | `key`                                         |
| `move`              | `x`, `y`                                      |
| `click`             | `x`, `y`, `button`, `pressed`                 |
| `scroll`            | `x`, `y`, `dx`, `dy`                          |

`key` is a stable identifier — the character for printable keys, the
`Key.space`-style repr for special keys. It exists for timing / n-gram
features. **Downstream code must not persist raw typed text.**

Mouse `move` events are throttled to roughly 50/sec (moves arriving
under 20 ms after the previous one are dropped). Clicks and scrolls
are never throttled.

## Configuration

Edit constants at the top of `streamguard/agent.py`:

- `USER_ID` — the enrolled user identity stamped onto every event and
  used as the Kafka message key.
- `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC` — broker and topic for `--sink kafka`.

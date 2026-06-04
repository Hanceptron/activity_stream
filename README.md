# KeySpark

Real-time typing-performance tracker for macOS. Captures keyboard and
mouse events, streams them through Kafka, derives per-minute metrics
and per-session fatigue analytics with Apache Spark, and renders a
live dashboard in the browser.

## Pipeline at a glance

```
agent (pynput, native macOS)
   │  JSON events
   ▼
Kafka  topic events.raw  (Docker, KRaft, single broker)
   │
   ▼
Spark Structured Streaming  (streaming_job.py)
   ├─► output/metrics/   one row per (user, 1-minute window)
   └─► output/events/    raw parsed event archive
                            │
                            ▼
                  Spark batch  (batch_job.py, scheduled every 5 min)
                            ├─► output/sessions/   session summaries + fatigue
                            ├─► output/baseline/   per-user mean / stddev
                            └─► output/heatmaps/   spatial heatmaps per range

FastAPI (api.py, :8000)  ── serves parquet as JSON  ─►  React dashboard (:5173)
```

The agent is a native process because the macOS Quartz event tap is
not reachable from inside a container. Everything downstream
(streaming, batch, API, frontend) is local.

See `explanation.md` for the design rationale and Big Data concepts
behind each layer.

## Components

| Path | What it is |
| --- | --- |
| `streamguard/agent.py` | pynput capture, JSON to stdout or Kafka, with macOS sleep/wake recovery |
| `streamguard/streaming_job.py` | Spark Structured Streaming: 1-minute tumbling windows + raw archive |
| `streamguard/batch_job.py` | Spark batch: sessions, fatigue, per-user baseline, spatial heatmaps |
| `streamguard/api.py` | FastAPI server exposing parquet outputs as JSON; owns the in-process batch scheduler |
| `frontend/` | Vite + React + Tailwind dashboard (polling-based, no WebSocket) |
| `docker-compose.yml` | Single-broker Kafka in KRaft mode |
| `startup-tmux.sh` | One-shot tmux launcher for all four foreground processes |
| `dev.sh` | uvicorn + Vite for the backend/frontend pair |

## macOS permissions

Grant both to whichever terminal launches Python (Terminal.app,
iTerm2, VS Code's integrated terminal, etc.):

- **System Settings → Privacy & Security → Accessibility**
- **System Settings → Privacy & Security → Input Monitoring**

Keyboard capture specifically depends on Input Monitoring. After
toggling either permission, fully quit and relaunch the terminal
(Cmd+Q, not just close the window) — macOS caches the permission
state at process start.

## Prerequisites

- Docker Desktop running.
- Java 17 or 21 on `PATH` (`java -version` should print something).
- Python 3.12 with `uv` (the project pins `requires-python = ">=3.12"`).
- `brew install tmux` if you want the recommended background-running
  mode in `startup-tmux.sh`.

## Setup

```sh
uv sync
```

Installs `pynput`, `confluent-kafka`, `pyspark`, `fastapi`,
`uvicorn`, `apscheduler`, `pandas`, `pyarrow`, and (on macOS)
`pyobjc-framework-Cocoa` into `.venv/`.

## Run the full pipeline

The fastest path to a live dashboard:

```sh
./startup-tmux.sh
```

This waits for Kafka on `localhost:9092`, then opens four tmux
windows: `agent`, `streaming`, `backend`, `frontend`. Each process
is wrapped in a `while true` restart loop, so a crashed Spark job or
a sleep-killed event tap recovers within ~5 s.

Open <http://localhost:5173>. The header's live dot is green when
the newest metric window is under two minutes old — that's the
end-to-end smoke test.

For the four-terminal foreground variant, the full operations
guide, the recovery recipes, and the "wipe everything" reset, see
`startup.md`.

## Run only the agent

The agent runs standalone for capture-only or enrollment workflows:

```sh
# stdout: one JSON event per line — useful to verify capture works
uv run python -m streamguard.agent

# kafka: produce to topic events.raw on localhost:9092, keyed by user
uv run python -m streamguard.agent --sink kafka

# record a session to a file for later analysis
uv run python -m streamguard.agent > session.jsonl
```

Stop with `Ctrl+C`. In Kafka mode the producer flushes before
exiting; on macOS sleep, in-flight events drain before the socket
dies and listeners are re-created on wake.

## Event shapes

All events carry `user` and `ts` (epoch seconds, float).

| `type`              | extra fields                                  |
| ------------------- | --------------------------------------------- |
| `key_down`/`key_up` | `key`                                         |
| `move`              | `x`, `y`                                      |
| `click`             | `x`, `y`, `button`, `pressed`                 |
| `scroll`            | `x`, `y`, `dx`, `dy`                          |

`key` is a stable identifier — the character for printable keys,
the `Key.space`-style repr for special keys. It exists for timing
and n-gram features. **Downstream code must not persist raw typed
text.**

Mouse `move` events are throttled to roughly 50/sec (moves arriving
under 20 ms after the previous one are dropped). Clicks and scrolls
are never throttled.

## Outputs

Spark writes everything under `output/` as parquet. The directories
are durable across runs (the `startup-tmux.sh` loops do not wipe
them); see `startup.md` for when and how to reset them.

| Directory | Producer | Shape |
| --- | --- | --- |
| `output/events/` | streaming | one row per parsed input event |
| `output/metrics/` | streaming | one row per (user, 1-minute window): keystrokes, words, corrections, clicks |
| `output/sessions/` | batch | one row per detected session: totals, slopes, `fatigue_index`, `fatigue_reliable` |
| `output/baseline/` | batch | one row per user: mean and stddev of each per-window metric |
| `output/heatmaps/{1h,6h,1d,3d,1w}/` | batch | one row per (cell_x, cell_y, type, user) for each preset range |
| `output/checkpoint/` | streaming | Spark checkpoint state (Kafka offsets, window state) |

## API

`streamguard/api.py` serves the parquet outputs as JSON over HTTP
on port 8000. All endpoints return an empty list before their
underlying parquet directory exists.

- `GET /api/metrics?minutes=60` — recent per-window counts for the chart.
- `GET /api/sessions` — 50 newest session summaries.
- `GET /api/baseline` — one row per user (mean/stddev per metric).
- `GET /api/heatmap?range={1h|6h|1d|3d|1w}` — cells for the spatial heatmap.
- `GET /api/batch_status` — last successful batch run timestamp.

The API also owns the in-process batch scheduler (every 5 minutes),
so there is no separate batch terminal in the normal run.

## Configuration

Edit constants at the top of the relevant module:

- `streamguard/agent.py` — `USER_ID`, `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`,
  `MOVE_MIN_INTERVAL`.
- `streamguard/streaming_job.py` — Kafka bootstrap, topic, output and
  checkpoint paths, watermark.
- `streamguard/batch_job.py` — `SESSION_GAP_SECONDS`, pause threshold,
  fatigue-reliability cutoff, `CELL_SIZE`, `HEATMAP_PRESETS`.

## Why native-only on macOS

The agent reads from the host's HID stream via the macOS Quartz
event tap (`pynput`). A Docker container has no access to host
input devices, so the agent runs as a native process on the Mac
being monitored. Kafka, Spark, the API, and the frontend have no
such constraint — Kafka runs in a container, Spark runs locally in
`local[*]` mode because we are processing one user's stream.

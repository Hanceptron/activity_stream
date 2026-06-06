# KeySpark

Real-time keyboard and mouse analytics for macOS. A native agent captures input
events, streams them through Kafka, and Apache Spark processes them with a Lambda
architecture: a Structured Streaming speed layer for live per-minute metrics and a
batch layer for session, baseline, heatmap, and per-day analytics. A liveness
classifier flags input automation (mouse jigglers, auto-clickers, keep-awake
tools), and a React dashboard renders everything live. No raw typed text is ever
stored.

See `index.md` for a file-by-file code map and where each setting lives.

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
              Spark batch  (batch_job.py, every 5 min)
                 ├─► output/sessions/             session summaries (totals)
                 ├─► output/baseline/             per-user mean / stddev
                 ├─► output/per_window/           flattened per-window table
                 ├─► output/day_minute_metrics/   per-day minute timeline
                 ├─► output/heatmap_by_day/       per-day spatial heatmap
                 └─► output/heatmaps/{1h,6h,1d,3d,1w}/  spatial heatmaps per range

Liveness classifier  (ml.py)  reads output/events/ + synthetic bots (botgen.py)
   └─► output/liveness.parquet   per-(user, day) human-vs-automation flags
       (scored after each batch; a day flags if >= 2 windows score >= 0.8)

FastAPI (api.py, :8000)  ── serves all parquet as JSON  ─►  React dashboard (:5173)
watchdog (watchdog.py)   ── bounces wedged streaming/backend panes after sleep/wake
```

The agent is a native process because the macOS Quartz event tap is not reachable
from inside a container. Everything downstream (streaming, batch, liveness, API,
frontend) is local.

## Components

| Path | What it is |
| --- | --- |
| `keyspark/agent.py` | pynput capture, JSON to stdout or Kafka, with macOS sleep/wake recovery |
| `keyspark/streaming_job.py` | Spark Structured Streaming: 1-minute tumbling windows + raw archive |
| `keyspark/batch_job.py` | Spark batch: sessions, per-user baseline, per-day rollups, spatial heatmaps |
| `keyspark/aggregations.py` | Shared per-window count expressions (single source of truth) |
| `keyspark/ml.py` | Liveness classifier: human vs input automation, scores `output/liveness.parquet` |
| `keyspark/botgen.py` | Synthetic bot event generator (non-human training class + demo injector) |
| `keyspark/benchmark.py` | Throughput / latency benchmark (batch + streaming) |
| `keyspark/watchdog.py` | Self-heal supervisor: bounces wedged streaming/batch panes after sleep/wake |
| `keyspark/api.py` | FastAPI server exposing parquet as JSON; owns the in-process batch + liveness scheduler |
| `frontend/` | Vite + React + Tailwind dashboard (polling-based, no WebSocket) |
| `docker-compose.yml` | Single-broker Kafka in KRaft mode |
| `startup-tmux.sh` | One-shot tmux launcher (agent, streaming, backend, frontend, watchdog) |
| `dev.sh` | uvicorn + Vite for the backend/frontend pair |

## macOS permissions

Grant both to whichever terminal launches Python (Terminal.app, iTerm2, VS Code's
integrated terminal, etc.):

- **System Settings -> Privacy & Security -> Accessibility**
- **System Settings -> Privacy & Security -> Input Monitoring**

Keyboard capture specifically depends on Input Monitoring. After toggling either
permission, fully quit and relaunch the terminal (Cmd+Q, not just close the
window) - macOS caches the permission state at process start.

## Prerequisites

- Docker Desktop running.
- Java 17 or 21 on `PATH` (`java -version` should print something).
- Python 3.12 with `uv` (the project pins `requires-python = ">=3.12"`).
- `brew install tmux` if you want the recommended background-running mode in
  `startup-tmux.sh`.

## Setup

```sh
uv sync
```

Installs `pynput`, `confluent-kafka`, `pyspark`, `fastapi`, `uvicorn`,
`apscheduler`, `pandas`, `pyarrow`, `scikit-learn`, `joblib`, and (on macOS)
`pyobjc-framework-Cocoa` into `.venv/`.

## Run the full pipeline

The fastest path to a live dashboard:

```sh
./startup-tmux.sh
```

This waits for Kafka on `localhost:9092`, then opens five tmux windows: `agent`,
`streaming`, `backend`, `frontend`, `watchdog`. Each long-running process is
wrapped in a `while true` restart loop, so a crashed Spark job or a sleep-killed
event tap recovers within ~5 s; the watchdog additionally bounces the streaming
and backend panes when they are alive but wedged after a host sleep/wake.

Open <http://localhost:5173>. The header's live dot is green when the newest metric
window is under two minutes old - that's the end-to-end smoke test.

For the foreground variant, the full operations guide, the recovery recipes, and
the "wipe everything" reset, see `startup.md`.

## Run only the agent

The agent runs standalone for capture-only or enrollment workflows:

```sh
# stdout: one JSON event per line - useful to verify capture works
uv run python -m keyspark.agent

# kafka: produce to topic events.raw on localhost:9092, keyed by user
uv run python -m keyspark.agent --sink kafka

# record a session to a file for later analysis
uv run python -m keyspark.agent > session.jsonl
```

Stop with `Ctrl+C`. In Kafka mode the producer flushes before exiting; on macOS
sleep, in-flight events drain before the socket dies and the process respawns on
wake with a fresh event tap.

## Event shapes

All events carry `user` and `ts` (epoch seconds, float).

| `type`              | extra fields                                  |
| ------------------- | --------------------------------------------- |
| `key_down`/`key_up` | `key`                                         |
| `move`              | `x`, `y`                                      |
| `click`             | `x`, `y`, `button`, `pressed`                 |
| `scroll`            | `x`, `y`, `dx`, `dy`                          |

`key` is a stable identifier - the character for printable keys, the
`Key.space`-style repr for special keys. It exists for timing and shape features.
**Downstream code must not persist raw typed text.**

Mouse `move` events are throttled to roughly 50/sec (moves arriving under 20 ms
after the previous one are dropped). Clicks and scrolls are never throttled.

## Outputs

Spark and the liveness model write everything under `output/` as parquet. The
directories are durable across runs (the `startup-tmux.sh` loops do not wipe them);
see `startup.md` for when and how to reset them.

| Path | Producer | Shape |
| --- | --- | --- |
| `output/events/` | streaming | one row per parsed input event |
| `output/metrics/` | streaming | one row per (user, 1-minute window): keystrokes, words, corrections, clicks |
| `output/sessions/` | batch | one row per session: start/end, window count, the four count totals |
| `output/baseline/` | batch | one row per user: mean and stddev of each per-window metric |
| `output/per_window/` | batch | flattened (session, window, user) counts - the table `ml.py` reads |
| `output/day_minute_metrics/` | batch | one row per (day, minute, user): counts + mouse moves, for the calendar timeline |
| `output/heatmap_by_day/` | batch | one row per (day, cell, type, user) for the per-day heatmap |
| `output/heatmaps/{1h,6h,1d,3d,1w}/` | batch | one row per (cell_x, cell_y, type, user) per preset range |
| `output/liveness.parquet` | ml | one row per (user, day): human-vs-automation flag + score |
| `output/models/` | ml | the trained classifier and `metrics.json` |
| `output/checkpoint/` | streaming | Spark checkpoint state (Kafka offsets, window state) |

## Liveness detection (human vs input automation)

`keyspark/ml.py` trains a random-forest classifier that labels each one-minute
window as a real person or input automation (mouse jiggler, auto-typer,
auto-clicker, keep-awake tool) from seven cross-modal shape features (interval and
mouse-step regularity, key diversity, mouse fraction). The human class is the real
event archive; the non-human class is synthetic events from `keyspark/botgen.py`,
grounded in how real keep-active tools behave. Both classes run through the same
featurizer, so there is no train/serve skew. After each batch the model scores
every window into `output/liveness.parquet`; a day is flagged when at least two of
its windows score >= 0.8, and the dashboard calendar colors that day red.

```sh
uv run python -m keyspark.ml train      # fit + persist the model
uv run python -m keyspark.ml evaluate   # stratified hold-out metrics -> metrics.json
uv run python -m keyspark.ml score      # write output/liveness.parquet
uv run python -m keyspark.botgen demo --kind jiggler --duration 180  # inject a demo bot into Kafka
```

The non-human class is synthetic, so the held-out scores partly reflect separating
real data from our own generator; validating against real captured automation is
future work.

## Benchmark

```sh
uv run python -m keyspark.benchmark batch       # batch analytical throughput (events/s)
uv run python -m keyspark.benchmark streaming   # streaming throughput (rows/s) + micro-batch latency
```

Both read only `output/events/` and write nothing into the live outputs.

## API

`keyspark/api.py` serves the parquet outputs as JSON over HTTP on port 8000. All
endpoints return an empty list (or `{"available": false}`) before their underlying
data exists.

- `GET /api/metrics?minutes=60` - recent per-window counts for the chart.
- `GET /api/sessions` - newest session summaries (up to 2000, spanning the calendar).
- `GET /api/baseline` - one row per user (mean/stddev per metric).
- `GET /api/heatmap?range={1h|6h|1d|3d|1w}` - cells for a preset spatial heatmap.
- `GET /api/day_metrics?day=YYYY-MM-DD&user=...` - per-minute timeline for one day.
- `GET /api/heatmap_day?day=YYYY-MM-DD&user=...` - spatial heatmap for one day.
- `GET /api/display` - primary-screen grid bounds for framing the heatmap.
- `GET /api/batch_status` - last batch run timestamp/status.
- `GET /api/health` - streaming + batch freshness snapshot.
- `GET /api/ml/metrics` - held-out liveness classifier metrics.
- `GET /api/liveness?user=...` - per-(user, day) automation flags (the red calendar days).

The API also owns the in-process scheduler that runs the batch job and liveness
scoring every 5 minutes, so there is no separate batch terminal in the normal run.

## Configuration

Every tunable constant carries a `# tune:` note at the top of its module; run
`grep -rn '# tune:' keyspark/` to list them all. The main ones:

- `keyspark/agent.py` - `USER_ID`, `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`,
  `MOVE_MIN_INTERVAL`, and the watchdog timings.
- `keyspark/streaming_job.py` - `WATERMARK`, `WINDOW_DURATION`, the trigger
  cadences, output and checkpoint paths.
- `keyspark/batch_job.py` - `SESSION_GAP_SECONDS`, `WINDOW_SIZE`, `CELL_SIZE`,
  `HEATMAP_PRESETS`.
- `keyspark/ml.py` - `WINDOW_THRESHOLD`, `MIN_FLAG_WINDOWS`, `MIN_WINDOW_EVENTS`,
  and the random-forest hyperparameters in `_make_model`.
- `keyspark/watchdog.py` - `CHECK_INTERVAL`, `STREAM_STALE_SECONDS`,
  `BATCH_STALE_SECONDS`, `RESTART_COOLDOWN`, `STARTUP_GRACE`.

## Why native-only on macOS

The agent reads from the host's HID stream via the macOS Quartz event tap
(`pynput`). A Docker container has no access to host input devices, so the agent
runs as a native process on the Mac being monitored. Kafka, Spark, the API, and
the frontend have no such constraint - Kafka runs in a container, and Spark runs
locally in `local[*]` mode because we are processing one user's stream.

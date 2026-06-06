# KeySpark - code map (index)

A file-by-file map for navigating the codebase. For setup and run instructions
see `README.md`; for operations/recovery see `startup.md`. Every tunable constant
in `keyspark/` carries a `# tune:` note - run `grep -rn '# tune:' keyspark/` to
list them all.

## Data flow

```
agent.py ──JSON──► Kafka (events.raw) ──► streaming_job.py
                                              ├─► output/metrics/   (live per-minute counts)
                                              └─► output/events/    (raw archive)
output/events/ ──► batch_job.py ──► output/{sessions,baseline,per_window,
                                            day_minute_metrics,heatmap_by_day,heatmaps}/
output/events/ + botgen.py ──► ml.py ──► output/liveness.parquet
everything under output/ ──► api.py (:8000) ──► frontend (:5173)
watchdog.py ──► restarts wedged streaming/backend tmux panes after sleep/wake
```

## Python modules (`keyspark/`)

Each module is organized with `# ----- Section -----` headers; the "sections"
column lists them top to bottom.

### `agent.py` - input capture
- **Role:** pynput keyboard/mouse capture on macOS; emits one JSON event per
  action to stdout or Kafka. Exits-and-respawns on wake (the event tap dies on
  sleep); HID-idle watchdog catches a silently dead tap.
- **Sections:** Settings, Sinks, pynput listeners, macOS wake/sleep observer +
  HID idle + display size, Main loop.
- **Settings:** `USER_ID`, `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `MOVE_MIN_INTERVAL`,
  `WATCHDOG_ACTIVE_SECONDS`, `WATCHDOG_SILENT_SECONDS`, `WATCHDOG_CHECK_INTERVAL`.
- **Writes:** event stream (stdout/Kafka) + `output/display.json`.

### `aggregations.py` - shared counts
- **Role:** single source of truth for the four per-window counts (keystrokes,
  words, corrections, clicks), used by both streaming and batch.
- **Settings:** `WORD_KEYS`, `CORRECTION_KEYS`. Entry point: `event_count_exprs()`.

### `streaming_job.py` - speed layer (Spark Structured Streaming)
- **Role:** consume `events.raw`, parse with an explicit schema + watermark, run
  two queries off one stream (A: per-minute counts -> metrics/; B: raw -> events/).
- **Sections:** Settings, Spark session + event schema, Pipeline (Read from Kafka,
  Parse + watermark, Query A metrics, Query B archive, Run loop).
- **Settings:** `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, `KAFKA_PACKAGE`, the 4 paths,
  `WATERMARK`, `WINDOW_DURATION`, trigger cadences (30 s / 60 s inline).
- **Reads:** Kafka `events.raw`. **Writes:** `output/metrics/`, `output/events/`,
  `output/checkpoint/{metrics,events}`.

### `batch_job.py` - batch layer (order-dependent analytics)
- **Role:** read the full event archive, sessionize (gap-and-island), and write
  session summaries, per-user baseline, the flattened per-window table, per-day
  rollups, and spatial heatmaps. Runs every 5 min from the API scheduler.
- **Sections:** Settings, Spark session, Event archive reader (schema guard),
  Sessionization + per-window metrics, Spatial heatmaps + per-day rollups, One
  batch pass (`compute_all`).
- **Settings:** the 7 output paths, `WINDOW_SIZE`, `SESSION_GAP_SECONDS`,
  `CELL_SIZE`, `HEATMAP_PRESETS`.
- **Reads:** `output/events/` (explicit part-file list, conforming-int64 only).
  **Writes:** `output/{sessions,baseline,per_window,day_minute_metrics,
  heatmap_by_day,heatmaps}/`.

### `ml.py` - liveness classifier (human vs input automation)
- **Role:** per-window RandomForest classifier; human = real archive, non-human =
  synthetic (botgen); scores per-(user, day) flags for the calendar.
- **Sections:** Settings, Feature engineering, Model, Train / evaluate, Scoring /
  inference, CLI.
- **Settings:** `FEATURES` (7 shape features), `WINDOW_THRESHOLD`,
  `MIN_FLAG_WINDOWS`, `MIN_WINDOW_EVENTS`, `MIN_ROWS`, `HUMAN_EXCLUDE_USERS`,
  `HUMAN_EXCLUDE_DAYS`, hyperparameters in `_make_model`, `test_size` in `evaluate`.
- **Reads:** `output/events/` + `botgen.synthetic_event_frame()`.
  **Writes:** `output/models/` (model + `metrics.json`), `output/liveness.parquet`.
- **CLI:** `train` | `evaluate` | `predict` | `score`.

### `botgen.py` - synthetic non-human class + demo injector
- **Role:** generate bot events (jiggler / typer / keep_awake / clicker) grounded
  in real keep-active tools (fixed geometry, single keep-awake key, jittered
  timing). Used as ml's non-human class and as a live demo.
- **Sections:** Settings, Event generation, Parquet I/O, Demo + seed, CLI.
- **Settings:** `KINDS`, `_BASE_RANGE`, `_JITTER_RANGE`, `CONTAMINATION`,
  `_MOVE_PATTERNS`, `_KEEPAWAKE_KEYS`, demo `--rate`/`--duration`.
- **CLI:** `demo` (inject into Kafka) | `seed` (write a backdated day into
  `output/events/`).

### `benchmark.py` - throughput / latency
- **Role:** measure batch events/s and streaming rows/s + micro-batch latency over
  the archive; writes nothing into the live outputs.
- **Settings:** `BENCH_CHECKPOINT`, `EVENTS_GLOB`, `STAGE_DIR`,
  `maxFilesPerTrigger` (inline). **CLI:** `batch` | `streaming`.

### `api.py` - serving layer + scheduler
- **Role:** FastAPI serving parquet as JSON; owns the in-process scheduler that
  runs `compute_all` + liveness scoring every 5 min and the sleep/wake `os._exit`
  recovery.
- **Sections:** Settings, Batch run state, Spark liveness probe + batch runner,
  App lifespan, Parquet -> JSON helpers, Endpoints (live metrics / batch analytics
  / batch health / liveness).
- **Settings:** the output paths, `DEFAULT_DISPLAY`, `SESSIONS_LIMIT`,
  `HEATMAP_RANGES`, `REFRESH_INTERVAL_SEC`.

### `watchdog.py` - self-heal supervisor
- **Role:** external process that bounces the streaming/backend tmux panes when
  they are alive but wedged (Spark RPC dead after sleep), via wake event + a
  freshness poll. Separate process so it never shares the wedged JVM.
- **Sections:** Settings, macOS wake observer, Probes (HID/API/restart), Wedged
  checks, Main loop.
- **Settings:** `SESSION`, `STREAMING_WINDOW`, `BACKEND_WINDOW`, `API_BASE`,
  `CHECK_INTERVAL`, `HID_ACTIVE_SECONDS`, `STREAM_STALE_SECONDS`,
  `BATCH_STALE_SECONDS`, `RESTART_COOLDOWN`, `STARTUP_GRACE`, `WAKE_SETTLE_SECONDS`
  (all overridable via `KEYSPARK_*` env).

## Outputs (`output/`)

| Path | Producer | Shape |
| --- | --- | --- |
| `events/` | streaming | one row per parsed input event (the archive) |
| `metrics/` | streaming | (user, 1-min window): keystrokes, words, corrections, clicks |
| `sessions/` | batch | (session, user): start/end, window count, count totals |
| `baseline/` | batch | (user): mean + stddev per metric |
| `per_window/` | batch | flattened (session, window, user) counts (ml's input) |
| `day_minute_metrics/` | batch | (day, minute, user): counts + mouse moves |
| `heatmap_by_day/` | batch | (day, cell, type, user) |
| `heatmaps/{1h,6h,1d,3d,1w}/` | batch | (cell_x, cell_y, type, user) per range |
| `liveness.parquet` | ml | (user, day): `nonhuman` flag + `score` |
| `models/` | ml | trained classifier + `metrics.json` |
| `checkpoint/` | streaming | Spark checkpoints (Kafka offsets, window state) |
| `display.json` | agent | primary screen size in points |

## API endpoints (`api.py`, :8000)

`/api/metrics` `/api/sessions` `/api/baseline` `/api/heatmap` `/api/day_metrics`
`/api/heatmap_day` `/api/display` `/api/batch_status` `/api/health`
`/api/ml/metrics` `/api/liveness` (see README "API" for params and shapes).

## Frontend (`frontend/src/`)

Vite + React + Tailwind, polling-based. `App.jsx` orchestrates polling and layout;
`usePolling.js` / `useNow.js` are the hooks; `utils.js` has shared helpers; the
`components/` render the panels - notably `MonthCalendar.jsx` (colors automation
days red from `/api/liveness`), `Header.jsx` (the live dot), `MetricsChart.jsx`,
`Heatmap.jsx`, `SessionTable.jsx`, `DayDetailPanel.jsx`, and the metric cards.

## Scripts & ops

- `startup-tmux.sh` - launches all five panes (agent, streaming, backend,
  frontend, watchdog), each under `run-with-backoff.sh`.
- `dev.sh` - backend + frontend only.
- `docker-compose.yml` - single-broker Kafka (KRaft).
- `scripts/seed_demo_jiggler.py` - seed a flagged demo day.

## Quick commands

```sh
./startup-tmux.sh                              # run the whole pipeline
uv run python -m keyspark.agent --sink kafka  # capture only
uv run python -m keyspark.ml evaluate         # liveness hold-out metrics
uv run python -m keyspark.benchmark streaming # throughput + latency
grep -rn '# tune:' keyspark/                  # list every tunable setting
```

## Where do I change...?

- **streaming window / watermark** -> `streaming_job.py` `WINDOW_DURATION`, `WATERMARK`
- **session gap** -> `batch_job.py` `SESSION_GAP_SECONDS`
- **heatmap resolution / ranges** -> `batch_job.py` `CELL_SIZE`, `HEATMAP_PRESETS`
- **automation flag sensitivity** -> `ml.py` `WINDOW_THRESHOLD`, `MIN_FLAG_WINDOWS`
- **model hyperparameters** -> `ml.py` `_make_model`
- **batch refresh cadence** -> `api.py` `REFRESH_INTERVAL_SEC`
- **watchdog timings** -> `watchdog.py` (or `KEYSPARK_*` env vars)
- **user id / Kafka address** -> `agent.py` `USER_ID`, `KAFKA_BOOTSTRAP`

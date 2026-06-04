# KeySpark - Explanation

## What this project is

KeySpark is a typing performance tracker. It records every
keyboard and mouse event from a user's machine, ships them through a
Kafka topic as JSON, and runs an Apache Spark Structured Streaming
job that groups the events into one-minute windows per user and
computes six signals per window: total keystrokes, words (spaces
pressed), corrections (backspace and delete), mouse clicks,
flight-time standard deviation, and the number of long pauses
between consecutive keystrokes. The per-window rows are written to
parquet files on disk.

The system has three running parts:

- A native macOS recording agent at `streamguard/agent.py`.
- A single Kafka broker running in Docker (`docker-compose.yml`).
- A Spark streaming job running locally (`streamguard/streaming_job.py`).

The agent runs on the host because it needs raw access to the
keyboard and mouse devices. Kafka runs in a container because it
does not. Spark runs locally in `local[*]` mode because we are
processing one user's stream and do not need a cluster.

## Kafka infrastructure

### What it does

Kafka is the inbox between the recording agent and the Spark job.
The agent does not talk to Spark directly; it appends each event to a
Kafka topic called `events.raw`, keyed by user id. The Spark job
reads from that topic on its own schedule. If the Spark job is not
running, Kafka keeps the events on disk until it is. If the producer
and the consumer run at different speeds, Kafka absorbs the
difference.

We run a single Kafka broker in KRaft mode. KRaft is Kafka's
self-contained consensus mode that replaces the older requirement of
running a separate ZooKeeper service alongside the broker. For a
one-broker development setup this means one container instead of two.

### The Big Data concept

Kafka is a distributed, append-only log. Producers write messages to
the end of a topic. Each topic is split into partitions, and
messages within a partition are strictly ordered. Consumers read
forward from a tracked offset that they (or the framework reading on
their behalf) commit back to Kafka. This is the standard substrate
for streaming systems for three reasons:

1. It decouples producers from consumers in both time and rate.
2. It persists data so consumers can be restarted, replayed, or
   added later without losing messages.
3. It serves as the "unbounded stream" that Spark Structured
   Streaming treats as a continuously growing input table.

### Where it lives

- `docker-compose.yml` lines 1-19: the entire Kafka service.
- Line 3 pins the broker image (`apache/kafka:3.8.1`).
- Lines 8-13 set this node's identity, roles (broker plus
  controller, in one process), listeners, and the controller quorum
  (just this one node).
- Line 11 advertises `localhost:9092` to clients so the host-side
  agent and Spark job can both connect at that address.
- Lines 16-18 set replication factors to 1 because there is only one
  broker.
- Line 19 enables auto-creation of `events.raw` on first write.

## Spark streaming job

### What it does

The streaming job consumes the `events.raw` topic, parses each
payload into typed columns with a 30-second watermark on event
time, and then splits into two independent streaming queries that
share the same parsed stream.

- **Query A - per-window counts.** Groups events into one-minute
  tumbling windows per user and aggregates four counts per window:
  `keystrokes` (events where `type == "key_down"`), `words`
  (`key_down` events where `key` is `" "` or `"Key.space"`),
  `corrections` (`key_down` events where `key` is `Key.backspace`
  or `Key.delete`), and `clicks` (events where `type == "click"`).
  Writes one row per (user, window) to parquet at `output/metrics/`.
- **Query B - raw event archive.** Writes the parsed events with no
  aggregation to parquet at `output/events/`. This is the durable,
  query-friendly archive that the batch job reads.

The `words` filter accepts both `" "` and `"Key.space"` because
pynput on macOS reports the space bar as the special key
`Key.space` rather than the literal space character. Matching only
on `" "` would leave the count stuck at zero.

Each query has its own checkpoint directory under
`output/checkpoint/`. After both queries are started, the
application blocks on `spark.streams.awaitAnyTermination()`, which
returns as soon as either query stops.

The order-dependent rhythm metrics (`flight_time_std` and
`long_pause_count`) and the spatial heatmap are not computed in
streaming; they live in the batch job because they need either
full event ordering (`lag`) or arbitrary time-range filtering, and
both are awkward on an unbounded stream.

### The Big Data concepts

**Structured Streaming and the unbounded-table model.** In Spark
Structured Streaming, an incoming stream is modelled as a table that
keeps growing forever. The same DataFrame operations used on a
finite table (`select`, `filter`, `groupBy`, `agg`) work on this
unbounded table. Spark runs them incrementally as new rows arrive.
The code we write looks almost identical to a batch job; the
difference is `readStream` instead of `read` and `writeStream`
instead of `write`.

**Kafka streaming source.** `spark.readStream.format("kafka")` tells
Spark to pull rows from a Kafka topic. Each row has fixed columns
including `key`, `value`, `topic`, `partition`, `offset`, and
`timestamp`. The user's JSON payload lives in `value` as raw bytes.
We cast it to a string and parse it with an explicit schema, which
avoids the cost and unpredictability of letting Spark infer the
schema from samples.

**Event time and watermarks.** There are two clocks in a streaming
system: the wall-clock time when Spark processes a record
(processing time), and the time the event actually happened on the
user's machine (event time, our `ts` column). Our windows are
defined on event time, which is the right choice for behaviour
metrics: a "typing minute" is one minute of real keyboard activity,
not one minute of Spark processing. Late and out-of-order events
are normal in streaming, so we set a watermark of 30 seconds. Spark
uses the watermark to decide when a window can be finalised and its
state freed: after 30 seconds past the window's end, any further
late events for that window are dropped.

**Tumbling windows.** A tumbling window is a fixed-width,
non-overlapping window. `window(event_time, "1 minute")` buckets
each event into exactly one one-minute window. Grouping by
`(window, user)` makes each window per user its own aggregation
cell, and Spark counts matching events within it.

**Append output mode.** Structured Streaming offers three output
modes: `complete` (re-emit the full result table every batch),
`update` (emit only rows that changed), and `append` (emit each row
exactly once, when it is finalised). The parquet file sink only
supports `append`. Append on a windowed aggregation is only allowed
when there is a watermark, because without one Spark can never tell
that a window's count is final and could keep emitting revised
versions of the same row forever. With our 30-second watermark,
Spark waits until the watermark has passed each window's end, then
emits that window's row once and discards its state. That is why
each (user, one-minute window) appears in `output/metrics/` exactly
once.

**Multiple streaming queries in one application.** A single
`SparkSession` can run any number of `writeStream` queries
concurrently. Each `.start()` returns a `StreamingQuery` handle
that runs independently, with its own micro-batch trigger and its
own checkpoint directory. `spark.streams.awaitAnyTermination()`
blocks the main thread until any one of the running queries
terminates, whether cleanly or due to an error. We use it to keep
the application alive while both queries run.

**Raw event archive as batch input.** Query B writes the parsed
events to parquet with no transformation. That gives the future
batch job a complete, ordered, time-bounded record of every event.
Because the batch job operates on a bounded DataFrame, it can apply
a `lag` window function over the full event ordering with no
batch-boundary blind spots, so flight-time gaps can be computed
exactly. The streaming job's responsibility ends at "land every
event durably and produce the simple counts"; anything that needs
ordering across the whole session is the batch job's responsibility.

**Parquet file sink with checkpointing.** Both queries use the
built-in parquet file sink, the columnar format that is the
de-facto standard for analytics storage. For each query, Spark
records in its checkpoint directory the Kafka offsets it has
consumed and, for Query A, the state of any open windowed
aggregations. On restart, each query resumes from its last
committed point. The parquet file sink commits each batch
atomically via a `_spark_metadata` log, so the output is end-to-end
exactly-once.

### Where it lives

- `streamguard/streaming_job.py` lines 21-34: hardcoded constants,
  including the Kafka connector package coordinate (line 26) which
  must match the installed pyspark version exactly and the two
  output and two checkpoint paths (lines 28-31).
- Lines 37-45: `SparkSession` in `local[*]` mode, with the
  `spark-sql-kafka-0-10` connector requested through
  `spark.jars.packages`.
- Lines 48-61: the explicit JSON schema (`type`, `key`, `x`, `y`,
  `button`, `pressed`, `dx`, `dy`, `user`, `ts`).
- Lines 68-75: the Kafka `readStream` source.
- Lines 77-83: JSON parse, `ts` to timestamp conversion, and the
  30-second watermark declaration. This `parsed` DataFrame is the
  shared input to both queries.
- Lines 86-105: Query A's windowed aggregation: one-minute
  tumbling windows grouped by user, four count aggregations, and
  the flat output projection.
- Lines 107-114: Query A's parquet `writeStream` to
  `output/metrics/` in append mode, with its own checkpoint
  directory.
- Lines 116-123: Query B's parquet `writeStream` over the raw
  parsed events to `output/events/` in append mode, with its own
  checkpoint directory.
- Line 125: `spark.streams.awaitAnyTermination()` keeps the
  application alive while both queries run.

## Spatial heatmap

### What it does

For each preset time range - 1 hour, 6 hours, 1 day, 3 days,
1 week - the batch job filters mouse events to that window, bins
each event into a `CELL_SIZE`-pixel grid cell on the screen,
counts events per `(cell, type, user)`, and writes one parquet
directory per preset under `output/heatmaps/{1h,6h,1d,3d,1w}/`.
The dashboard's time-range selector picks which directory to read.

### Why it lives in the batch job and not in streaming

An earlier design ran the heatmap as a streaming aggregation in
complete mode. That gave a single rolling all-time-cumulative
heatmap - useful as a live monitoring view, useless as a
statistical view, because you cannot ask "which cells were hot in
the last hour" of an aggregation that has no time dimension.

Implementing time-range filtering in streaming would mean either
keeping a separate windowed aggregation per preset (expensive
state, awkward emission semantics) or maintaining a single
high-cardinality hour-bucket aggregation and summing it down at
read time (awkward to write, awkward to read). The batch job is
the natural home: it already owns the full event archive at
`output/events/`, can scan it once and produce five filtered
heatmap views per run, and writes one tidy parquet directory per
preset.

### The Big Data concepts

**Spatial binning is windowing over coordinates.** Time windowing
groups events into time buckets; spatial binning does the same
thing over `x` and `y` by integer-dividing each coordinate by
`CELL_SIZE`. `groupBy("cell_x", "cell_y", "type", "user")` is the
spatial equivalent of `groupBy(window(event_time, "1 minute"),
"user")`. Both reduce a continuous stream into a small table of
cells.

**Relative-to-max time cutoff.** Each preset's filter is
`event_time >= max(event_time) - hours`. The cutoff is measured
backwards from the newest event in the archive, not from
wall-clock now. That way an archive recorded yesterday or last
week still produces populated heatmaps when the batch job runs
today, instead of silently emptying out because no events happened
in the last literal hour.

**One scan, five filters, five writes.** The events DataFrame is
cached at the top of `main()` so all five heatmap filters reuse
the same in-memory scan instead of re-reading the parquet archive
five times. For a week of recorded events (millions of rows) this
turns five sequential scans into one.

### Where it lives

- `streamguard/batch_job.py` line 19: `HEATMAPS_PATH`.
- Lines 26-33: `CELL_SIZE` and the `HEATMAP_PRESETS` list mapping
  each label to its hour count.
- Lines 197-214: `heatmap_for_range(events, hours, max_event_time)`
  - filter events to the last `hours` hours of `move` and `click`
  events, derive `cell_x` and `cell_y` by integer division, group
  by `(cell_x, cell_y, type, user)`, count.
- Lines 226-234 of `main()`: cache `events`, compute
  `max_event_time` once via `F.max("event_time")`, loop over
  `HEATMAP_PRESETS`, write each preset to
  `output/heatmaps/{name}/` in `overwrite` mode.

## Batch job - sessions, fatigue, and baseline

### Why batch and not streaming

Two of the original six metrics, `flight_time_std` and
`long_pause_count`, are computed from the time gap between
consecutive keystrokes. They are order-dependent: you cannot
compute the gap without knowing the previous keystroke's timestamp.
In Spark Structured Streaming, the SQL `lag` window function does
not see across streaming micro-batches, so a `lag`-based
aggregation on a live stream silently misses every gap that
straddles a batch boundary. The streaming job therefore writes
every parsed event to parquet at `output/events/`, and the batch
job, which sees the full ordered event log as a bounded DataFrame,
computes these metrics exactly.

This is the standard Lambda-style split: the streaming layer
produces fast, append-only per-window counts that a live dashboard
can read. The batch layer is the source of truth for the
order-dependent and session-shaped analytics - rhythm, pauses,
session boundaries, fatigue trends, and the per-user baseline. The
batch job runs over the same parquet archive and overwrites its
outputs each run, so re-running it is always safe.

### What `lag` does here

`lag(col).over(Window.partitionBy("user").orderBy("event_time"))`
returns, for every row, the value of `col` from the row immediately
before it within the same user, in event-time order. The code does:

```
gap_seconds = event_time.cast("double")
              - lag("event_time").over(user_order).cast("double")
```

For each `key_down` event, `gap_seconds` is the number of seconds
since that user's previous keystroke. The first event per user gets
`null`, which both `stddev` and conditional `count` ignore. Because
the batch job has the entire event log in one bounded DataFrame,
this is exact - there are no batch-boundary blind spots.

### How sessions are detected

Conceptually a session is a contiguous burst of activity separated
from the next burst by at least `SESSION_GAP_SECONDS` (5 minutes)
of idle time. Spark has a built-in `session_window` function for
exactly this shape, but it has two practical limitations that bite
here: it cannot share a `groupBy` with the per-minute `window`
(Spark refuses on cartesian-product grounds), and even when
materialized as a column its per-event struct values do not
partition correctly under `Window.partitionBy`. So we sessionize
manually with the standard gap-and-island pattern:

1. Order events per user by `event_time`.
2. For each event, look at the previous event's timestamp with
   `lag(event_time)` over that window.
3. Set a flag to 1 if the gap is missing (first event) or larger
   than `SESSION_GAP_SECONDS`; otherwise 0.
4. The running cumulative sum of those flags is a stable integer
   `session_id` per event: 1 for the first session, 2 for the
   second, and so on.

`session_id` is then a plain integer column that both `groupBy`
(for the count and rhythm aggregations) and `Window.partitionBy`
(for the `row_number`-based `window_idx`) handle correctly.

### What `regr_slope` measures

`regr_slope(y, x)` is the SQL aggregate for the slope of a linear
regression of `y` on `x`. Given a session's per-window rows
numbered `window_idx = 0, 1, 2, ...`,
`regr_slope(keystrokes, window_idx)` fits a line through those
points and returns its slope - in other words, the average
per-window change in keystrokes across the session.

We compute four slopes per session:

- `keystrokes_slope`: positive means typing accelerates over the
  session, negative means it slows down.
- `corrections_slope`: positive means backspaces grow over the
  session (more mistakes).
- `rhythm_slope`: positive means the flight-time standard deviation
  grows (rhythm becomes more uneven).
- `pause_slope`: positive means long pauses become more frequent.

### Fatigue

```
fatigue_index = -keystrokes_slope
                + corrections_slope
                + rhythm_slope
                + pause_slope
```

The sign flip on `keystrokes_slope` makes "typing speeds up" reduce
the index, while the other three terms add to the index when their
metrics worsen. So a positive `fatigue_index` is a session whose
performance degrades over time. A negative `fatigue_index` is a
session hitting flow - faster, fewer mistakes, steadier rhythm,
fewer pauses.

### Why a reliability flag

A linear regression on two or three points is meaningless: a
two-point line fits exactly, and small numbers of windows produce
very noisy slopes. `fatigue_reliable = window_count >= 5` gives any
downstream consumer (a dashboard, an alert rule) a single boolean
to hide or grey out fatigue scores from short sessions where the
trend is statistical noise.

### What the baseline is for

`output/baseline/` holds one row per user with the mean and
standard deviation of each of the six per-window metrics. The
intent is to score future windows against the user's own history:
given a fresh window's `keystrokes`, the z-score
`(keystrokes - keystrokes_mean) / keystrokes_std` says how unusual
that minute is for that user. A dashboard or anomaly check would
read this file alongside the streaming output.

### Where it lives

- `streamguard/batch_job.py` lines 13-20: constants - the input
  and output paths, the per-window size, the session-gap threshold
  in seconds, the pause threshold, and the fatigue-reliability
  cutoff.
- Lines 23-30: `SparkSession` in `local[*]` mode. No Kafka
  connector is needed here.
- Lines 33-114: `per_window_metrics` - the lag-based
  sessionization that produces an integer `session_id` per event
  (lines 47-66), the four count aggregations on all events
  (lines 68-84), the lag-based gap calculation on `key_down` events
  only (lines 91-98), the two rhythm aggregations (lines 100-112),
  and the left join into a single per-window row (line 114).
- Lines 117-160: `session_summary` - the `row_number` based
  `window_idx` partitioned by `(session_id, user)` (lines 119-124),
  the per-session aggregation including the four `regr_slope` calls
  (lines 126-148), and the derived `fatigue_index` and
  `fatigue_reliable` columns (lines 149-159).
- Lines 163-181: `user_baseline` - mean and stddev of each of the
  six metrics per user, plus a `computed_at` timestamp.
- Lines 184-194: `main` - read events once, cache the per-window
  result because it is reused by both downstream writers, then
  overwrite `output/sessions/` and `output/baseline/`.

## Backend API

### Why a small backend exists

A React dashboard running in the browser cannot read parquet files
directly - parquet is a columnar binary format meant for analytics
engines, not for JavaScript `fetch` calls. `streamguard/api.py` is
a thin FastAPI app that opens those parquet files and serves their
rows as JSON over HTTP, started with
`uv run uvicorn streamguard.api:app --reload`.

### The endpoints

All four return a JSON list of row dicts and respond with an empty
list when the underlying parquet directory does not exist yet (the
batch job's outputs only appear after the batch job has run).

- `GET /api/metrics?minutes=60` - per-window count metrics from
  `output/metrics/`, filtered to the last `minutes` minutes and
  sorted oldest first. Drives the live time-series chart.
- `GET /api/sessions` - the 50 most recent session summaries from
  `output/sessions/`, newest first, with totals, slopes,
  `fatigue_index`, and `fatigue_reliable`.
- `GET /api/baseline` - all rows from `output/baseline/`, one per
  user. The dashboard z-scores live metrics against the user's own
  history.
- `GET /api/heatmap` - all rows from `output/heatmap/`, one per
  `(cell, type, user)`. Feeds the spatial heatmap visualisation.

### Intentional simplicity

No caching, no service layer, no SQL or DuckDB tier. Each request
opens the parquet directory with `pandas.read_parquet` and
converts the rows via `df.to_json`, which handles two annoying
details for us in one step: timestamps become ISO strings and
`NaN` values become JSON `null`. Wasteful for a production
workload, perfectly adequate for a single-user demo where requests
are infrequent and parquet sizes are small. CORS is enabled for
all origins so the React dev server on a different port can call
this API freely.

One subtle point: Spark stores parquet timestamps in UTC and
pandas reads them as tz-naive `datetime64[ns]` holding the raw UTC
value, so the metrics-window threshold is computed with
`pd.Timestamp.now("UTC").tz_localize(None)` to stay UTC-vs-UTC.

## Frontend dashboard

### Architecture

The dashboard is a Vite + React single-page app in `frontend/`. It
opens four polling loops with `useEffect` and `setInterval`, one
per backend endpoint, and re-renders whenever any of the four
state slots receives a new response. There is no WebSocket: HTTP
polling is two lines of code per stream, the data is small, and
the freshness needed by the demo (5 to 30 seconds depending on
endpoint) is well within what plain polling can deliver. A custom
`usePolling(url, intervalMs)` hook hides the fetch / interval /
cleanup pattern so each call site is a single line.

In development the Vite dev server proxies `/api/*` to
`http://localhost:8000`, so the React app on port 5173 makes
same-origin requests and CORS never gets involved.

### The five sections

The page is a single column with five vertically stacked sections:

1. **Header** - title plus a live indicator dot. Green when the
   most recent metric window is less than two minutes old, red
   otherwise. Tells you at a glance whether the streaming
   pipeline is actually flowing.
2. **Metric cards** - four cards (keystrokes per minute, words per
   minute, corrections, clicks) showing the most recent
   one-minute window's values, with the user's baseline mean as a
   small subtitle for context.
3. **Metrics chart** - a Recharts `LineChart` of the last 60
   minutes, with three colored lines for keystrokes (blue), words
   (green), and corrections (red). The x-axis is window start
   time formatted as `HH:MM`.
4. **Two heatmaps side by side** - "Movement" (blue) and "Clicks"
   (red), each an SVG mapping the heatmap rows from the API. A
   small `1h / 6h / 1d / 3d / 1w` selector above the two heatmaps
   picks which precomputed time range the API serves; changing it
   re-runs `usePolling` against `/api/heatmap?range=<X>`.
5. **Sessions list** - the 20 most recent sessions from the batch
   job, newest first, with start time, duration, total
   keystrokes, and fatigue index. Sessions whose
   `fatigue_reliable` flag is `false` show "insufficient data"
   instead of a noisy slope from a 2-or-3-window session.

### Heatmap rendering

Each heatmap is a single `<svg>` with a viewBox sized to the
maximum `(cell_x, cell_y)` actually present in the data (or a
16:9 baseline of 48 x 27 cells if the data is empty). One `<rect>`
per row from `/api/heatmap` (filtered to the chosen type) is drawn
at its `(cell_x, cell_y)` with `width=1, height=1`. Opacity is

```
opacity = log(count + 1) / log(max_count + 1)
```

The log scaling is the important detail. Mouse traffic is heavy
tailed: a few cells get thousands of hits (where you rest the
cursor) while most cells get only a handful. Linear opacity would
make the rare cells invisible. With log scaling, a cell with 5
hits still gets a meaningful share of the maximum opacity, so the
full spatial distribution is readable.

### Why Tailwind and no UI library

Tailwind utility classes mean every visual choice is right next to
the element it affects in the JSX. There is no `Button.module.css`
to open, no `<Card variant="elevated">` to look up in a component
library's docs, no theme provider to thread state through. A
student reading any component sees the layout, the colors, the
spacing, and the responsive behaviour as plain class names on the
element. The cost is some repetition (the same `bg-zinc-800
rounded-lg p-4 border border-zinc-700` appears on every panel),
which is fine for a small dashboard and easy to factor later if
needed.

### Where it lives

- `frontend/vite.config.js`: Vite config, the Tailwind plugin, and
  the `/api` dev proxy.
- `frontend/src/usePolling.js`: the polling hook (fetch + interval
  + cleanup).
- `frontend/src/utils.js`: `parseUtc` that appends `Z` to ISO
  strings so the browser interprets them as UTC.
- `frontend/src/App.jsx`: top-level layout and the four polling
  intervals.
- `frontend/src/components/Header.jsx`: title plus live indicator.
- `frontend/src/components/MetricCards.jsx`: four metric cards.
- `frontend/src/components/MetricsChart.jsx`: Recharts line chart.
- `frontend/src/components/Heatmap.jsx`: SVG heatmap with
  log-scaled opacity.
- `frontend/src/components/RangeSelector.jsx`: the 1h / 6h / 1d /
  3d / 1w button group above the two heatmaps. Holding the
  selected value in `App.jsx` state means changing it flips the
  URL passed to `usePolling`, which triggers a fresh fetch.
- `frontend/src/components/SessionsList.jsx`: most-recent-sessions
  table with the fatigue coloring rule.

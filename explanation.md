# StreamGuard - Explanation

## What this project is

StreamGuard is a typing performance tracker. It records every
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

The streaming job consumes the `events.raw` topic, treats the JSON
payloads as structured rows, and writes per-(user, one-minute window)
metric rows to parquet on disk.

Pipeline:

1. Subscribe to the Kafka topic as a streaming source.
2. Decode the Kafka value bytes as a UTF-8 string and parse it as
   JSON against an explicit schema.
3. Turn the `ts` field (epoch seconds) into a proper Spark timestamp.
4. Declare a 30-second watermark on that timestamp.
5. Hand each micro-batch to a `foreachBatch` handler.
6. In the handler, group events into one-minute tumbling windows per
   user and compute six per-window metrics (the four event counts
   plus the two order-dependent rhythm metrics described in the next
   section).
7. Append the combined rows to parquet, with a checkpoint directory
   beside it.

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
are normal in streaming, so we set a watermark of 30 seconds. This
tells Spark: after 30 seconds past a window's end, assume no more
late events will arrive for that window and free its state from
memory. Without a watermark, Spark would have to keep every open
window in state forever.

**Tumbling windows.** A tumbling window is a fixed-width,
non-overlapping window. `window(event_time, "1 minute")` buckets
each event into exactly one one-minute window. Grouping by
`(window, user)` makes each window per user its own aggregation
cell, and Spark counts matching events within it.

**Parquet output with checkpointing.** The job writes its output
rows as parquet, the columnar format that is the de-facto standard
for analytics-friendly storage. The checkpoint location is required
for any streaming query: Spark stores the Kafka offsets it has
consumed there. If the job is restarted, it resumes from the last
committed offsets with no skipped events. The actual write happens
inside the `foreachBatch` handler described in the next section.

### Where it lives

- `streamguard/streaming_job.py` lines 28-40: hardcoded constants,
  including the Kafka connector package coordinate (line 33) which
  must match the installed pyspark version exactly.
- Lines 43-51: `SparkSession` in `local[*]` mode, with the
  `spark-sql-kafka-0-10` connector requested through
  `spark.jars.packages`.
- Lines 54-67: the explicit JSON schema (`type`, `key`, `x`, `y`,
  `button`, `pressed`, `dx`, `dy`, `user`, `ts`).
- Lines 128-135: the Kafka `readStream` source.
- Lines 137-143: JSON parse, `ts` to timestamp conversion, and the
  30-second watermark declaration.
- Lines 77-87: the one-minute tumbling window grouped by user, and
  the four count aggregations (inside `process_batch`).
- Lines 145-151: the `writeStream` that hands each micro-batch to
  `process_batch` via `foreachBatch`, with `checkpointLocation`
  pointing at the checkpoint directory.

## Order-dependent metrics

### What it does

The first four metrics (keystrokes, words, corrections, clicks)
evaluate each event on its own: count the events that match a
condition. The two new metrics need to know the gap in time between
consecutive `key_down` events:

- `flight_time_std`: standard deviation of those gaps within a
  window. A steady typist has a small standard deviation; a bursty
  typist has a large one.
- `long_pause_count`: how many of those gaps were longer than two
  seconds within a window. A rough flow indicator - more long pauses
  means more thinking or searching, less continuous typing.

Both metrics require ordering the events by time and looking at
adjacent pairs, which a plain `groupBy` cannot express.

### The Big Data concepts

**SQL window functions and `lag`.** A SQL window function computes a
value for each row by looking at a set of other rows defined by a
window specification - a `PARTITION BY` clause to group rows, plus
an `ORDER BY` clause to order them within the group. Unlike
`groupBy + agg`, the result still has one output row per input row,
with an extra column derived from the partition's ordering.
`lag(col)` is the simplest window function: it reads the value of
`col` from the row immediately before the current one in the
partition's order, or `null` if there is no such row. In our code:

```
user_order = Window.partitionBy("user").orderBy("ts")
gap = col("ts") - lag("ts").over(user_order)
```

For each `key_down` event, `gap` becomes the number of seconds
since the previous `key_down` event for the same user. The first
event in each partition gets a `null` gap, which Spark's aggregate
functions (`stddev`, `count`) ignore automatically. `stddev` over
the `gap` column then gives the standard deviation of flight times
within the window, and a conditional `count` gives the number of
gaps over the long-pause threshold.

**Why `foreachBatch`.** Structured Streaming's incremental
windowed aggregation does not allow `lag` (or any SQL window
function) on the unbounded stream directly: `lag` needs a
deterministic global ordering, which an open-ended stream cannot
provide. `foreachBatch` is the escape hatch. Each micro-batch is
delivered to a user-defined function as an ordinary bounded
DataFrame, on which the full set of Spark SQL operations is
available - including window functions. Our `process_batch`
function uses `lag` and standard aggregation to compute all six
metrics on the batch, then writes the resulting rows to parquet
itself.

**Honest limitation: lag across micro-batch boundaries.** The `lag`
function only sees rows present in the current micro-batch. If a
`key_down` event lands at the start of one batch and the previous
`key_down` event landed at the end of the previous batch, the gap
between them is not computed - the first event of the batch gets a
`null` gap, the same as the very first event of a session. So a
small number of flight-time samples are missed at every batch
boundary, and the standard deviation and long-pause count are
slightly biased downward. Fixing this correctly would require
carrying the last-seen `ts` per user across batches in user-managed
state, which is more machinery than this demo needs.

### Where it lives

- `streamguard/streaming_job.py` line 40: the
  `LONG_PAUSE_SECONDS = 2.0` threshold.
- Lines 70-121: the `process_batch` function used by `foreachBatch`.
- Lines 77-87: the four count aggregations on the batch.
- Line 93: the `Window.partitionBy("user").orderBy("ts")`
  specification.
- Line 96: the `gap = ts - lag(ts)` calculation.
- Lines 97-101: the second `groupBy` over the same one-minute
  tumbling window that aggregates `gap` into `flight_time_std` and
  `long_pause_count`.
- Lines 104-117: the left join that combines counts and rhythm into
  the six-metric output row, projected to flat
  `(window_start, window_end, user, ...)` columns.
- Line 119: the per-batch parquet append.
- Line 147: the `.foreachBatch(process_batch)` call on the
  `writeStream` that wires the handler in.

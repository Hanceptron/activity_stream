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

The streaming job consumes the `events.raw` topic, parses each
payload into typed columns with a 30-second watermark on event time,
and then splits into two independent streaming queries that share
the same parsed stream.

- **Query A - per-window counts.** Groups events into one-minute
  tumbling windows per user and aggregates four counts per window:
  `keystrokes` (events where `type == "key_down"`), `words`
  (`key_down` events where `key == " "`), `corrections` (`key_down`
  events where `key` is `Key.backspace` or `Key.delete`), and
  `clicks` (events where `type == "click"`). Writes one row per
  (user, window) to parquet at `output/metrics/`.
- **Query B - raw event archive.** Writes the parsed events with no
  aggregation to parquet at `output/events/`. This is the durable,
  query-friendly archive that the upcoming batch job will read.

Each query has its own checkpoint directory under
`output/checkpoint/`. After both queries are started, the
application blocks on `spark.streams.awaitAnyTermination()`, which
returns as soon as either query stops.

The two order-dependent rhythm metrics (`flight_time_std` and
`long_pause_count`) are no longer computed here; they will be
produced by an upcoming batch job that reads `output/events/`,
where `lag` has full visibility into the event ordering and is
exactly correct.

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

- `streamguard/streaming_job.py` lines 19-32: hardcoded constants,
  including the Kafka connector package coordinate (line 24) which
  must match the installed pyspark version exactly, and the two
  output and two checkpoint paths (lines 26-29).
- Lines 35-43: `SparkSession` in `local[*]` mode, with the
  `spark-sql-kafka-0-10` connector requested through
  `spark.jars.packages`.
- Lines 46-59: the explicit JSON schema (`type`, `key`, `x`, `y`,
  `button`, `pressed`, `dx`, `dy`, `user`, `ts`).
- Lines 66-73: the Kafka `readStream` source.
- Lines 75-81: JSON parse, `ts` to timestamp conversion, and the
  30-second watermark declaration. This `parsed` DataFrame is the
  shared input to both queries.
- Lines 83-103: Query A's windowed aggregation: one-minute tumbling
  windows grouped by user, four count aggregations, and the flat
  output projection.
- Lines 105-112: Query A's parquet `writeStream` to
  `output/metrics/` in append mode, with its own checkpoint
  directory.
- Lines 114-121: Query B's parquet `writeStream` over the raw
  parsed events to `output/events/` in append mode, with its own
  checkpoint directory.
- Line 123: `spark.streams.awaitAnyTermination()` keeps the
  application alive while both queries run.

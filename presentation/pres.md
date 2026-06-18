# KeySpark - Presentation Explanation Sheet

Your study sheet for the BDA5011 deck (`presentation/keyspark.pptx`). For each slide:
what it says in one line, then every keyword/concept explained plainly. Read this
to understand and defend your own deck. The most-likely exam terms have an
"If asked" note. A quick glossary for last-minute review is at the very end.

Rule of thumb for the whole talk: **the pipeline (Kafka + Spark) is the graded
contribution; the automation detector is the application that gives it a purpose.**

---

## Slide 1 - Title

**Says:** Project name, subtitle, you (Murat Emirhan Aykut), Bahcesehir University, BDA5011.

- **KeySpark** - the project: it captures keyboard and mouse events as a live stream and analyzes them.
- **Input automation** - software/hardware that fakes human presence (mouse jigglers, auto-clickers, keep-awake tools). This is what the application detects.
- **Streaming pipeline** - data is processed as it arrives (a flow), not loaded once from a file (a batch).

---

## Slide 2 - Problem and motivation

**Says:** Input events are an unbounded high-rate stream; we detect fake-presence automation; this is a Big Data streaming problem; it is privacy-safe.

- **Event** - one recorded action: a key press, a mouse move, a click, a scroll.
- **Unbounded stream** - data that never "ends"; it keeps arriving, so you cannot wait for all of it before computing.
- **High-rate / bursty** - many events per second, in uneven spikes (typing fast, then idle).
- **Liveness** - is a real human at the keyboard, or a tool? "Liveness detection" = human vs non-human.
- **Out-of-order arrival** - events can reach the system in a different order than they happened.
- **Content-free / privacy by construction** - we store which key and when, never the actual text typed. Privacy is built into the data shape, not added as a rule afterward.

**If asked "why is this Big Data?":** unbounded high-rate data, event-time windowing, out-of-order arrival, producer-consumer rate mismatch, fault tolerance, and a fast-vs-complete processing split - the core streaming-systems concerns.

---

## Slide 3 - Related work

**Says:** Four research threads KeySpark builds on. You do not need every citation; know what each column is.

- **Streaming foundations** - the systems papers behind your stack: Kafka (the log), Spark RDDs (the engine), Structured Streaming (stream as a table), Lambda architecture (speed + batch).
- **Streams of interaction data** - other projects that pipe user-interaction events (e.g. browser clickstreams) through Kafka + Spark. Your closest structural cousins.
- **Keystroke + mouse dynamics** - research showing typing/mouse timing alone (no content) reveals things: identity, emotion, fatigue. Justifies using timing as a signal.
- **Detecting automation** - prior bot/automation detection from input behavior. BeCAPTCHA-Mouse is notable: it also trains partly on synthetic bot data, the same honest caveat you carry.
- **Your niche** - a live streaming pipeline computing a content-free human-vs-automation signal over both keyboard and mouse.

---

## Slide 4 - Dataset and visualization

**Says:** Self-collected archive, ~2.65 million events over 25 active days, one user, frozen 18 Jun 2026; content-free; chart of daily activity.

- **Self-collected** - you recorded your own real usage; you did not download a dataset.
- **Frozen / data freeze** - you locked the dataset at a fixed point (18 Jun 2026) so the numbers in the deck and paper stop moving and stay reproducible.
- **Active day** - a day you actually used the machine (idle days are not counted).
- **Schema** - the fixed list of fields each event has: `type key x y button pressed dx dy user ts`. "Explicit schema" = you declared these fields up front instead of letting Spark guess.
- **Throttling (~50/s)** - mouse moves are capped at about 50 per second so raw volume stays bounded; clicks and scrolls are never throttled (they are rarer and more meaningful).
- **The chart** - real keystroke and click counts per day. The point is the natural irregularity of human activity (including a near-empty day, Jun 7). That irregularity is the signal the detector later uses.

**If asked why the chart and dashboard calendar differ:** the chart is absolute keystroke+click counts from `output/metrics`; the calendar shows active-minutes normalized to your median day, with red overriding for automation. Two different views of the same archive.

---

## Slide 5 - System architecture (the centerpiece)

**Says:** One stream in, three kinds of output: speed (seconds), batch (minutes), liveness (the application). Follow the diagram left to right.

- **Capture agent (pynput)** - the program that taps OS keyboard/mouse events. `pynput` is the Python library for this. It must run natively on the host, not in a container, because it needs real OS input access.
- **Kafka (topic `events.raw`, KRaft, single broker)** - the durable log the agent writes every event into. (Kafka explained in detail on Slide 6.)
  - **Topic** - a named stream/log inside Kafka; yours is `events.raw`.
  - **Broker** - a Kafka server process; you run one ("single broker").
  - **KRaft** - Kafka's newer self-managed mode; it removes the old separate ZooKeeper service so Kafka manages its own metadata.
- **Structured Streaming** - the Spark job that reads Kafka and computes results continuously (Slide 8-9).
- **metrics/** - per-minute counts (keystrokes, words, corrections, clicks), written as Parquet. The "speed layer" output.
- **events/** - a verbatim Parquet copy of every event. The raw archive that feeds the batch layer and the model.
- **Spark batch job** - runs every 5 minutes over the whole archive to compute heavier, accurate aggregates (Slide 6, 17).
- **sessions / baseline / heatmaps / per-day metrics** - the batch outputs (work sessions, your normal behavior, where the mouse went, daily totals).
- **Liveness classifier + synthetic bots** - the application branch (Slide 10).
- **FastAPI -> Dashboard (React + Vite)** - FastAPI is the Python web server that turns Parquet into JSON; React/Vite is the browser dashboard that displays it.
- **Three lanes (speed / batch / liveness)** - the color coding: cyan = seconds, magenta = minutes, red = the detector riding on both.

---

## Slide 6 - Why this stack (the slide that earns the grade)

**Says:** Each component justified with a one-line "why."

- **Apache Kafka = the ingestion log.**
  - **Commit log** - an append-only, ordered, durable record of events. You only ever add to the end; nothing is edited.
  - **Decoupling** - the agent (producer) writes to Kafka and never waits on Spark (consumer). If Spark is slow or down, the agent keeps capturing.
  - **Replay** - because every event sits in the log at a numbered position (offset), Spark can re-read from any past point after a crash or a code change.
  - **Offset** - the sequential ID of a record's position in the log; consumers track offsets to know what they have read.
  - **Backpressure / buffer** - the log absorbs bursts so a fast producer does not overwhelm a slower consumer.
  - **If asked "why not write to a DB or a plain queue?":** a direct DB insert or a fire-and-forget queue cannot replay and gives no buffer; Kafka's offsets are also the foundation of Spark's exactly-once guarantee.
- **Spark Structured Streaming = the speed layer.**
  - Correct **event-time** windowing under late/out-of-order data (a naive consumer loop gets this wrong).
  - **Exactly-once** output via checkpointing (Slide 9).
  - One parsed stream feeding two outputs (metrics + archive), no duplicated ingestion.
  - **local[*] to cluster** - the same code scales from one machine to many.
- **Spark batch = the batch layer (Lambda).**
  - Some analytics (sessions, baselines) depend on the previous event, which streaming micro-batches cannot see across. So the stream stores everything, and a batch pass over the full archive is the accurate source of truth.
  - **lag()** - a SQL function that reads the previous row; it cannot look across micro-batch boundaries, which is exactly why sessionization must be batch.
- **Parquet = the serving store.**
  - **Columnar** - stores data column-by-column, so reading a few fields over millions of rows is cheap (perfect for dashboards).
  - **Compressed + splittable** - small on disk and readable in parallel.
  - It is Spark's native file sink with exactly-once commits, so it adds no extra service to run.
- **Lambda architecture** - the overall pattern: a fast "speed layer" (recent, approximate) plus a slow "batch layer" (complete, accurate), reconciled by a serving layer. Name it explicitly; it is course-relevant.

---

## Slide 7 - Why not MongoDB (known exam question)

**Says:** Mongo is a fine database but the wrong shape for this workload. Concrete, honest.

- **MongoDB / document store** - a database that stores flexible JSON-like documents, optimized for reading and updating individual records.
- **CRUD** - Create, Read, Update, Delete: the mutable single-record operations Mongo is good at.
- **Secondary index / B-tree** - lookup structures that make "find document where field = X" fast; they are pure overhead on a workload that never updates and always scans everything.
- **What this workload actually is** - append-only (nothing is ever updated), high-rate, and queried by windowed aggregation over all rows, not by point lookups.
- **What you would have to rebuild on Mongo** - event-time windowing, watermarks, exactly-once, and replay. Mongo has none of these; you would reimplement them in application code.
- **Honest concession** - for low-rate, mutable CRUD data, Mongo would be simpler and better. It is the wrong tool here specifically because this is high-rate, append-only, windowed analytics - the course's workload.

**One-liner:** "Not a bad database, the wrong shape. Mongo does point reads/writes of mutable documents; I need windowed scans over an append-only stream, which is Kafka + Spark + Parquet."

---

## Slide 8 - Streaming deep-dive 1: event time and the 5 s watermark

**Says:** Windows are defined on when events happened, not when Spark saw them; a watermark handles lateness and makes the file output legal.

- **Event time** - the timestamp inside the event (when the key was actually pressed). Windows use this.
- **Processing time** - when Spark happens to process the event. Irrelevant to the result here.
- **Tumbling window** - fixed-size, non-overlapping time buckets; yours are 1 minute. Each event belongs to exactly one window.
- **Late / out-of-order event** - an event whose event-time falls in a window that already ended on the wall clock. Example on the slide: pressed 10:04:57, arrived 10:05:03.
- **Watermark** - a moving cutoff equal to (max event-time seen) minus a delay (5 seconds). It means "I will still accept events at least this recent." Once the watermark passes a window's end, that window is final: its result is written and its memory is freed.
- **Append mode** - the output writes each window's result row once, when final, and never rewrites it. The Parquet file sink only supports append.
- **Why the watermark is mandatory** - append can only emit a window once it is provably closed. Without a watermark Spark could never declare a window closed, so append would emit nothing.

**If asked "why 5 seconds?":** one local broker produces little disorder, and the watermark delays output. At 30 s the first result took ~90 s to appear; 5 s tolerates the real lateness and keeps the dashboard live.

---

## Slide 9 - Streaming deep-dive 2: two queries, exactly-once

**Says:** One parsed stream feeds two writers; output is exactly-once on disk; the job survives sleep/wake.

- **Parse once, two sinks** - the Kafka bytes are parsed a single time, then two queries branch off: Query A (per-minute metrics) and Query B (verbatim archive). No duplicated reading.
- **Sink** - the destination a query writes to (here, Parquet folders).
- **Trigger (30 s / 60 s)** - how often a micro-batch runs and writes. Without triggers, every tiny micro-batch writes a file; you once had 100k+ tiny files. Triggers bound the file-creation rate.
- **Exactly-once** - every input event affects the output exactly one time: no loss, no duplicates. (Contrast: at-least-once can duplicate; at-most-once can lose.)
  - **Kafka offsets** - each micro-batch records the exact range of offsets it consumed.
  - **Checkpoint (WAL + state)** - Spark's saved progress on disk: the consumed offsets (a write-ahead log) plus the in-flight window counts (state). It survives restarts.
  - **WAL (write-ahead log)** - durable record of "I am about to process these offsets," written before processing, so recovery knows where it was.
  - **Atomic Parquet commit (`_spark_metadata`)** - the file sink writes a manifest listing only fully-committed files; readers honor it, so a half-written or duplicate file is never seen. A file logically exists only once committed.
  - **Restart = resume** - after any crash Spark replays from the checkpointed offsets and the sink ignores uncommitted files, so the result is identical to a run with no crash.
- **Sleep/wake hardening** - laptop sleep is the messy real-world case.
  - **failOnDataLoss=false** - if Kafka's log was truncated during sleep, the reader resets to the earliest available offset instead of crashing in a loop. Losing a few input events is cheaper than a permanent crash loop.
  - **Watchdog** - a supervisor that catches the "Spark process alive but internally wedged after sleep" case and restarts only the stuck component (more on Slide 17).

---

## Slide 10 - Liveness application (supporting, keep it short)

**Says:** Per-minute features describe the *shape* of activity; a RandomForest classifies human vs non-human; a day turns red on a sustained automation burst. The non-human class is synthetic - stated honestly.

- **Cross-modal features** - features that combine keyboard and mouse signals, computed per 1-minute window. "Shape, not volume" - they measure regularity and variety, never how much or what content.
  - **key_diversity** - distinct keys divided by total keystrokes; low for an auto-typer hammering one key.
  - **IEI (inter-event interval)** - the time gap between consecutive events.
  - **CV (coefficient of variation)** - standard deviation divided by mean; a unitless "how irregular is this." Low CV = very regular = machine-like; high CV = irregular = human-like. `ks_iei_cv`, `move_iei_cv`, `iei_cv` are CVs of keyboard, mouse, and all-event intervals.
  - **step_mean / step_cv** - average mouse move distance and its variability; a jiggler has rigid, repetitive steps (low step_cv).
  - **mouse_fraction** - share of events that are mouse vs keyboard; near 1 for a jiggler, near 0 for a key-spamming tool.
- **RandomForest (300 trees, balanced classes)** - the classifier.
  - **Decision tree** - a series of yes/no splits on feature values ending in a class.
  - **RandomForest** - many such trees that vote; robust on tabular data, resists overfitting, and reports which features mattered.
  - **Balanced class weights** - human windows vastly outnumber non-human; "balanced" tells the model to value the rare class so it is not ignored.
- **Synthetic bots (botgen)** - the non-human training examples are generated, not captured: a jiggler, a keep-awake (F15-style), an auto-typer, an auto-clicker. Their cadence is jittered so the model learns the robotic *shape*, not a fixed timer.
  - **One featurizer for both classes** - real and synthetic events go through the same feature code, so there is no train-vs-serve mismatch.
- **The red-day rule** - a day is flagged when at least 2 windows score >= 0.8 (a sustained burst, not one odd minute). Surfaced on the dashboard calendar after each 5-minute batch.
- **Honest caveat** - the non-human class is synthetic, so this is evidence the features are discriminative, not a proven field detection rate.

---

## Slide 11 - Results and discussion

**Says:** Throughput first (the course weight), then the classifier; numbers are frozen and reported honestly.

- **Throughput** - how much data is processed per second.
  - **Batch ~7.0 x 10^5 events/s** - the batch layer does one pass with no streaming overhead, so it is fastest per event.
  - **Streaming ~2.0 x 10^5 rows/s** - lower because it trades raw speed for incremental, low-latency processing. This contrast *is* the Lambda tradeoff in numbers.
- **Latency** - delay.
  - **275 ms / micro-batch** - average time to process one streaming batch.
  - **~65 s end-to-end** - keypress to dashboard. This equals the 60 s window plus the 5 s watermark. It is a design floor (a per-minute number cannot exist before the minute ends), not engine slowness.
- **Liveness metrics (held-out test set):**
  - **Accuracy 0.994** - fraction of all predictions correct.
  - **Precision 0.970** - of the windows flagged automation, how many really were (high = few false alarms).
  - **Recall 0.994** - of the truly automated windows, how many we caught (high = few misses).
  - **F1 0.982** - single balance score combining precision and recall.
  - **ROC-AUC 1.000** - probability the model ranks a random automated window above a random human one; 1.0 means perfect separation. A near-perfect score is itself a reason to be cautious, which is the caveat.
  - **Stratified hold-out** - the model is tested on data it never trained on, with the class ratio preserved in train and test.
  - **Positive class** - the thing being detected = non-human (automation).
- **The caveat band** - the right-hand numbers partly measure separation from your own synthetic generator. You claim the features discriminate; you do not claim a real-world detection rate. Validation on real captured automation is future work.

---

## Slide 12 - Demonstration

**Says:** What you will show live after the slides.

- **Beat 1 - Type** - you type; within ~65 s the per-minute metrics, time series, and heatmap update. The audience watches the window + watermark latency happen.
- **Beat 2 - Inject a jiggler** - botgen pushes synthetic jiggler events into Kafka; a sustained burst scores non-human and the calendar day flips red. Shows the whole pipeline reacting end to end.
- **Beat 3 - Spark UI, Structured Streaming tab** - Spark's built-in monitoring page showing live input rate vs processing rate and micro-batch durations. The engine's own evidence, not your slides.
- Then a short code walk: the streaming job, the batch job, the featurizer.

---

## Slide 13 - Limitations and future work

**Says:** What this is not yet, stated plainly, with concrete next steps.

- **Single broker / user / machine (local[\*])** - one Kafka broker, one person's data, one computer. So Kafka **partitioning** (splitting a topic across servers for parallelism) and Spark **shuffle** (moving data between nodes, the main scaling cost) are never truly stressed.
- **Synthetic non-human class** - the held-out scores are inflated relative to automation in the wild.
- **Idle suppressors invisible** - tools like `caffeinate`, Amphetamine, or PowerToys Awake prevent sleep without sending any input (they hold an OS **power assertion**), so an input-based detector cannot see them by construction.
  - **Power assertion** - an OS flag an app holds to say "do not sleep"; it can be listed out-of-band, which is the proposed fix.
- **Latency floor** - ~65 s is tied to the 1-minute window; shorter windows mean lower latency but noisier labels.
- **Sleep that kills Spark's RPC** - costs a cold restart, not a few seconds. (**RPC** = the internal driver/executor communication that can die on sleep while the process stays alive.)
- **Future work** - validate on a real **hardware USB jiggler** (it emits genuine **HID events** the agent can capture, unlike software injection which macOS drops); ground botgen against real keep-active tools; probe OS power assertions to catch idle suppressors; go multi-user and clustered to exercise partitioning and shuffle.

---

## Slide 14 - Conclusion

**Says:** Four takeaways and thanks.

- Standard big-data parts, end to end: agent -> Kafka -> Spark -> Parquet -> FastAPI -> React.
- The **Lambda split is earned**, not decorative: cross-batch ordering forces a batch source of truth.
- Streaming **correctness is explicit**: event time, a 5 s watermark, append + checkpoints = exactly-once.
- The ML rides the pipeline and is reported **honestly** (content-free features, synthetic-class caveat named).

---

## Slide 15 - Backup divider

Just a section break before the backup slides (code, operations, Q&A) you pull up only if asked.

---

## Slide 16 - Backup: the actual code (`streaming_job.py`)

**Says:** The real parse -> window -> sink code, with the four load-bearing lines annotated. The professor opens code, so be ready.

- **from_json + explicit schema** - the Kafka value is a JSON string; you parse it with a declared schema (`event_schema()`) rather than letting Spark infer types. Predictable, cheap, typed.
- **withWatermark("event_time", "5 seconds")** - attaches the watermark (Slide 8) so windowed append is legal.
- **groupBy(window(event_time, "1 minute"), user)** - the tumbling window aggregation, grouped per user per minute.
- **writeStream ... format("parquet") ... checkpointLocation ... outputMode("append") ... trigger("30 seconds")** - the sink: write Parquet, keep a checkpoint for exactly-once, append finalized rows, run every 30 s. Query B is the same pattern without the aggregation, on a 60 s trigger.

---

## Slide 17 - Backup: operational anatomy

**Says:** How checkpoints, sleep/wake, batch internals, and serving actually work.

- **Checkpoint + commit** - per query, Spark persists Kafka offset ranges (WAL) and window state; the Parquet `_spark_metadata` manifest makes file commits atomic. Together = exactly-once.
- **Sleep/wake** - `failOnDataLoss=false` resets truncated offsets instead of crash-looping; every process runs in a restart loop; the watchdog reacts to the OS wake event, checks output freshness, probes the Spark RPC, and bounces only the wedged component.
- **Batch internals** - a 5-minute scheduler inside the API reads the full archive by file glob (independent of the streaming commit log). **Sessionization** uses **gap-and-island**: order each user's events by time, flag gaps over 5 minutes, then cumulative-sum the flags to assign session IDs.
  - **Sessionization** - grouping events into continuous work sessions separated by idle gaps.
  - **Gap-and-island** - the standard SQL trick for that grouping.
- **Serving** - FastAPI reads Parquet and serves JSON; React polls it. The calendar joins per-day metrics with liveness flags and renders flagged days red.

---

## Slide 18 - Backup: anticipated questions

Prepared answers for likely probes. Know these cold.

- **Why not Flink or Kafka Streams?** Flink is true per-event with lower latency, but your latency floor is the 1-minute window, so micro-batching costs nothing here; and Lambda needs a batch engine anyway, which Spark gives you with one engine and one API. (**Flink** = a per-event stream processor; **Kafka Streams** = a stream-processing library tied to Kafka.)
- **Why a 5 s watermark?** Little disorder from one local broker; 30 s made first results take ~90 s; 5 s balances lateness vs liveness.
- **Why 1-minute windows?** The product unit is "a minute of activity." Shorter = lower latency but noisier labels; longer = smoother but staler.
- **Why RandomForest, not deep learning?** 7 tabular features and modest data. It retrains in seconds, resists overfitting with balanced weights, and its feature importances explain its decisions.
- **How exactly is exactly-once achieved?** Offsets + state checkpointed per micro-batch; the file sink commits atomically via `_spark_metadata`; replay after failure reprocesses the same offsets without double-committing.
- **Could the agent write to Spark directly?** Then capture blocks on every consumer hiccup and nothing is replayable. The log decouples rates, buffers bursts, and makes restart and reprocessing free.

---

## 60-second glossary (last-minute review)

- **Kafka** - durable, ordered, replayable append-only log; decouples producer from consumer.
- **Offset** - numbered position of a record in the log; enables replay and exactly-once.
- **Spark Structured Streaming** - treats a live stream as a continuously growing table you query like batch.
- **Micro-batch** - Structured Streaming processes the stream in small timed batches.
- **Event time vs processing time** - when it happened vs when Spark saw it; windows use event time.
- **Tumbling window** - fixed-size, non-overlapping time buckets (1 minute here).
- **Watermark** - max event-time minus 5 s; lets Spark close windows, emit them once, and free state. Required for append.
- **Append mode** - write each window result once when final; the only mode the Parquet file sink supports.
- **Checkpoint** - saved offsets + window state on disk; enables exact resume after a crash.
- **Exactly-once** - every event affects output once; no loss, no duplicates.
- **Lambda architecture** - fast speed layer + accurate batch layer + serving layer.
- **Parquet** - columnar, compressed, splittable file format for analytical scans; Spark's native exactly-once sink.
- **Sessionization (gap-and-island)** - grouping events into work sessions split by idle gaps.
- **RandomForest** - ensemble of voting decision trees; robust on tabular features, gives importances.
- **CV (coefficient of variation)** - std/mean; low = regular = machine-like, high = irregular = human-like.
- **Precision vs recall** - false alarms vs misses; F1 balances them; ROC-AUC = separation quality (1.0 = perfect).
- **Throughput vs latency** - amount/second vs delay; your ~65 s end-to-end is the window + watermark by design.

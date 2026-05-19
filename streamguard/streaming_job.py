"""StreamGuard Spark Structured Streaming job.

Reads raw input events from Kafka, parses the JSON payloads, and
writes six typing-performance metrics per one-minute window per
user to parquet: four event counts plus two order-dependent rhythm
metrics computed inside a foreachBatch handler.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    lag,
    stddev,
    when,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructType,
)
from pyspark.sql.window import Window as W

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events.raw"

# Must match the installed pyspark version exactly. pyspark 4.1.1 is
# built against Scala 2.13, so the connector artifact is _2.13.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

OUTPUT_PATH = "output/metrics"
CHECKPOINT_PATH = "output/checkpoint"

WATERMARK = "30 seconds"
WINDOW_DURATION = "1 minute"
LONG_PAUSE_SECONDS = 2.0


def build_session():
    return (
        SparkSession.builder
        .appName("streamguard")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def event_schema():
    return (
        StructType()
        .add("type", StringType())
        .add("key", StringType())
        .add("x", LongType())
        .add("y", LongType())
        .add("button", StringType())
        .add("pressed", BooleanType())
        .add("dx", LongType())
        .add("dy", LongType())
        .add("user", StringType())
        .add("ts", DoubleType())
    )


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.persist()
    try:
        is_kd = col("type") == "key_down"

        counts = (
            batch_df.groupBy(window(col("event_time"), WINDOW_DURATION), col("user"))
            .agg(
                count(when(is_kd, 1)).alias("keystrokes"),
                count(when(is_kd & (col("key") == "Key.space"), 1)).alias("words"),
                count(
                    when(is_kd & col("key").isin("Key.backspace", "Key.delete"), 1)
                ).alias("corrections"),
                count(when(col("type") == "click", 1)).alias("clicks"),
            )
        )

        # Order-dependent metrics. lag over key_down events
        # partitioned by user and ordered by ts gives the previous
        # key_down event's ts; subtracting yields the gap (flight
        # time) for every key_down except the first per partition.
        user_order = W.partitionBy("user").orderBy("ts")
        rhythm = (
            batch_df.filter(is_kd)
            .withColumn("gap", col("ts") - lag("ts").over(user_order))
            .groupBy(window(col("event_time"), WINDOW_DURATION), col("user"))
            .agg(
                stddev(col("gap")).alias("flight_time_std"),
                count(when(col("gap") > LONG_PAUSE_SECONDS, 1)).alias("long_pause_count"),
            )
        )

        combined = (
            counts.join(rhythm, on=["window", "user"], how="left")
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user"),
                col("keystrokes"),
                col("words"),
                col("corrections"),
                col("clicks"),
                col("flight_time_std"),
                col("long_pause_count"),
            )
        )

        combined.write.mode("append").parquet(OUTPUT_PATH)
    finally:
        batch_df.unpersist()


def main():
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), event_schema()).alias("e"))
        .select("e.*")
        .withColumn("event_time", col("ts").cast("timestamp"))
        .withWatermark("event_time", WATERMARK)
    )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

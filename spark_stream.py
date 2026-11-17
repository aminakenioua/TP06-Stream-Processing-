
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    date_format
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)


spark = (
    SparkSession.builder
    .appName("CampusRealTimeAnalytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "Africa/Algiers")
# Monitor Spark processing rate
class RateListener(StreamingQueryListener):

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        progress = event.progress

        if progress.name == "checkin-window-rate":
            print("\n===== SPARK PROCESSING RATE =====")
            print("Processed events:", progress.numInputRows)

            print(
                "Input rate:",
                round(progress.inputRowsPerSecond, 2),
                "events/second"
            )

            print(
                "Processing rate:",
                round(progress.processedRowsPerSecond, 2),
                "events/second"
            )

    def onQueryTerminated(self, event):
        pass


spark.streams.addListener(RateListener())
schema = StructType([
    StructField("eventId", IntegerType(), True),
    StructField("studentId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("department", StringType(), True),
    StructField("eventTime", StringType(), True)
])


raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "earliest")
    .load()
)


json_events = raw.selectExpr(
    "CAST(value AS STRING) AS json"
)


parsed = (
    json_events
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)


checkins = parsed.filter(
    col("eventType") == "check-in"
)


counts = (
    checkins
    #.withWatermark("event_ts", "30 seconds")
    .withWatermark("event_ts", "2 minutes")
    .groupBy(
       # window(col("event_ts"), "1 minute"),
      #window(col("event_ts"), "2 minutes"),
      window(
    col("event_ts"),
       "2 minutes",
      "1 minute"
       ),
        col("department")
    )
    .count()
)


query = (
  counts.writeStream
    .queryName("checkin-window-rate")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/experiment-D")
    #.option("checkpointLocation", "./checkpoint/experiment-C")
    #.option("checkpointLocation", "./checkpoint/campus-events")
    .trigger(processingTime="5 seconds")
    .start()
)


#query.awaitTermination()

# --------------------------------------------------
# Task 5: Detect Late Check-ins
# --------------------------------------------------

late_checkins = (
    checkins
    .filter(
        date_format(col("event_ts"), "HH:mm:ss") > "08:15:00"
    )
    .select(
        "studentId",
        "department",
        "eventTime"
    )
)

# Start Late Check-in Alert Query
late_query = (
    late_checkins.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/late-alert")
    .trigger(processingTime="5 seconds")
    .start()
)


# Keep both queries running
spark.streams.awaitAnyTermination()
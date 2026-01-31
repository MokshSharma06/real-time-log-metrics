from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col,to_timestamp,window
from src.schema import LOG_SCHEMA
from src.utils import get_spark_session


from pyspark.sql.functions import from_json, col, to_timestamp, window
spark=get_spark_session()
# 1. Standard Source Setup
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "app-logs") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Schema and Timestamp Conversion
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), LOG_SCHEMA).alias("data")
).select("data.*")

df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("event_time"))
)

# 3. Watermark and Aggregation
# Using a 2-minute watermark as we discussed for your test
agg_df = df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("service_name")
    ) \
    .count()

# 4. Corrected Sink
query = agg_df.writeStream \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

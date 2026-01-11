from pyspark.sql.functions import from_json, col, to_timestamp
from src.utils import get_spark_session
from src.schema import LOG_SCHEMA

spark = get_spark_session()

bronze_df = spark.readStream \
    .format("delta") \
    .load("data/bronze/logs")


parsed_df = bronze_df.withColumn(
    "data",
    from_json(col("raw_value"), LOG_SCHEMA)
)

silver_df = parsed_df.select(
    col("data.event_id").alias("event_id"),
    col("data.service").alias("service_name"),
    col("data.status_code").alias("status_code"),
    to_timestamp(col("data.event_time")).alias("event_time"),
    col("ingestion_time"),
    col("kafka_partition"),
    col("kafka_offset")
)
silver_df = silver_df.filter(col("event_time").isNotNull())

query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "checkpoints/silver/logs") \
    .start("data/silver/logs")

query.awaitTermination()


# silver_df =spark.read \
#         .format('delta') \
#         .load('data/silver/logs')

# silver_df.show()
e
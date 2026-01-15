from pyspark.sql.functions import from_json, col, to_timestamp,expr
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
    col("data.event_id").alias("event_id"),  col("data.service").alias("service_name"),
    col("data.status_code").alias("status_code"),
    to_timestamp(col("data.event_time")).alias("event_time"),
    col("ingestion_time"),
    col("kafka_partition"),
    col("kafka_offset")
)
is_valid_expr = (
    col("event_id").isNotNull() & 
    col("event_time").isNotNull() & 
    (col("status_code") >= 100) & (col("status_code") <= 500)
)

is_late_expr = expr("event_time < ingestion_time - INTERVAL 10 MINUTES")
processed_stream = (
    silver_df
    .withColumn("is_valid", is_valid_expr)
    .withColumn("is_late", is_late_expr)
    .withWatermark("event_time", "24 hours") 
    .dropDuplicates(["event_id", "event_time"])
)
def multi_sink_writer(batch_df, batch_id):
    batch_df.persist()
    internal_flags = ["is_valid", "is_late"]

    # CLEAN DATA
    (batch_df
     .filter((col("is_valid") == True) & (col("is_late") == False))
     .drop(*internal_flags)
     .write.format("delta")
     .mode("append")
     .save("data/silver/clean"))

    # BAD DATA
    (batch_df
     .filter(col("is_valid") == False)
     .drop(*internal_flags)
     .write.format("delta")
     .mode("append")
     .save("data/silver/bad"))

    # LATE DATA 
    (batch_df
     .filter((col("is_valid") == True) & (col("is_late") == True))
     .drop(*internal_flags)
     .write.format("delta")
     .mode("append")
     .save("data/silver/late"))

    batch_df.unpersist()


query = processed_stream.writeStream \
    .foreachBatch(multi_sink_writer) \
    .trigger(availableNow=True) \
    .option("checkpointLocation", "checkpoints/silver/logs") \
    .start()

query.awaitTermination()


# silver_df =spark.read \
#         .format('delta') \
#         .load('data/silver/late')

# from pyspark.sql import functions as F

# target_id = "7b7511bf-9deb-451b-be8f-51b93d153e74"
# filtered_df = silver_df.filter(F.col("event_id") == target_id)

# filtered_df.show()

# silver_df.show(10000)

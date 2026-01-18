from pyspark.sql.functions import from_json, col, to_timestamp,expr
from src.utils import get_spark_session
from src.schema import LOG_SCHEMA
from delta.tables import DeltaTable
from src.transformations import classify_data
from pyspark.sql.functions import col


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
    .withColumn("schema_valid", is_valid_expr)
    .withColumn("is_late", is_late_expr)
    .withWatermark("event_time", "2 hours")
    .dropDuplicates(["event_id", "event_time"])
)
classified_stream = classify_data(processed_stream)

# paths for the sinks
clean_path = "data/silver/clean"
bad_path = "data/silver/bad"
late_path = "data/silver/late"

def multi_sink_writer(batch_df, batch_id):
    batch_df.persist()
    internal_flags = ["schema_valid", "is_late"]

    clean_data = batch_df.filter(col("record_status") == "CLEAN").drop(*internal_flags)
    bad_data   = batch_df.filter(col("record_status") == "BAD").drop(*internal_flags)
    late_data  = batch_df.filter(col("record_status") == "LATE").drop(*internal_flags)
    

    if not clean_data.isEmpty():
        if DeltaTable.isDeltaTable(spark, clean_path):
            target_table = DeltaTable.forPath(spark, clean_path)
            (target_table.alias("t")
             .merge(clean_data.alias("s"), "t.event_id = s.event_id")
             .whenNotMatchedInsertAll()
             .execute())
        else:
            clean_data.write.format("delta").mode("append").option("mergeSchema","true").save(clean_path)


    # BAD DATA
    bad_data.write.format("delta") \
    .mode("append") \
    .option("mergeSchema","true") \
    .save(bad_path)

    # LATE DATA 
    late_data.write.format("delta") \
    .mode("append") \
    .option("mergeSchema","true") \
    .save(late_path)

    batch_df.unpersist()

query = (
    classified_stream
    .writeStream
    .queryName("silver_layer")
    .foreachBatch(multi_sink_writer)
    .option("checkpointLocation", "checkpoints/silver")
    .outputMode("update")
    .start()
)

query.awaitTermination()



silver_df =spark.read \
        .format('delta') \
        .load('data/silver/late')

# from pyspark.sql import functions as F
# from datetime import date

# # target_id = "421bb00f-3e3f-4ee9-870d-234c0a160fed"
# # filtered_df=silver_df.filter(F.col("event_id")==target_id)
# # filtered_df = silver_df.filter(F.to_date(F.col("ingestion_time")) == F.current_date())
# silver_df.show()

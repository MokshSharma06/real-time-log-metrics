from pyspark.sql.functions import from_json, col, to_timestamp, expr
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from src.schema import LOG_SCHEMA
from src.transformations import classify_data
import yaml
from src.utils import load_config






def start_silver_stream(spark,config):
    kafka_conf = config["kafka"]
    base_paths = config["paths"]

    bronze_path = f"{base_paths['bronze']}/logs"
    checkpoint_path = f"{base_paths['checkpoints']['bronze']}/logs"
    silver_path = f"{base_paths['silver']}/logs"

    # -------------------- READ BRONZE --------------------
    bronze_df = (
        spark.readStream
        .format("delta")
        .load(bronze_path)
    )

    # -------------------- PARSE --------------------------
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

    # -------------------- VALIDATION FLAGS ----------------
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
        .withWatermark("event_time", "1 hours")
        .dropDuplicates(["event_id", "event_time"])
    )

    # -------------------- PHASE 4 CLASSIFICATION ----------
    classified_stream = classify_data(processed_stream)

    # -------------------- SINK PATHS ----------------------
    clean_path = "data/silver/clean"
    bad_path   = "data/silver/bad"
    late_path  = "data/silver/late"

    # -------------------- FOREACH BATCH WRITER ------------
    def multi_sink_writer(batch_df, batch_id):
        batch_df=batch_df.withColumn("_batch_id",F.lit(batch_id))
        batch_df.persist()

        internal_flags = ["schema_valid", "is_late"]

        clean_data = batch_df.filter(col("record_status") == "CLEAN").drop(*internal_flags)
        bad_data   = batch_df.filter(col("record_status") == "BAD").drop(*internal_flags)
        late_data  = batch_df.filter(col("record_status") == "LATE").drop(*internal_flags)

        # ---------- CLEAN (IDEMPOTENT MERGE) ----------
        if not clean_data.isEmpty():
            if DeltaTable.isDeltaTable(spark, clean_path):
                target = DeltaTable.forPath(spark, clean_path)
                (
                    target.alias("t")
                    .merge(clean_data.alias("s"), "t.event_id = s.event_id")
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                clean_data.write \
                    .format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save(clean_path)

        # ---------- BAD ----------
        if not bad_data.isEmpty():
            if DeltaTable.isDeltaTable(spark,bad_path):
                target_bad=DeltaTable.forPath(spark, bad_path)
                #Remove any records from a previous failed attempt of this same batch
                target_bad.delete(F.col("_batch_id")==batch_id)
    
            bad_data.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(bad_path)

        # ---------- LATE ----------
        if not late_data.isEmpty():
            if DeltaTable.isDeltaTable(spark,late_path):
                target_late = DeltaTable.forPath(spark, late_path)
                target_late.delete(F.col("_batch_id") == batch_id)


            late_data.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(late_path)

        batch_df.unpersist()

    # -------------------- START STREAM --------------------
    query = (
        classified_stream
        .writeStream
        .queryName("silver_layer")
        .foreachBatch(multi_sink_writer)
        .option("checkpointLocation", "checkpoints/silver")
        .outputMode("update")
        .start()
    )

    return query
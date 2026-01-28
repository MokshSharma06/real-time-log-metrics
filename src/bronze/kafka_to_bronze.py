from pyspark.sql.functions import col, current_timestamp, to_date, hour
from databricks.sdk.runtime import dbutils
from src.utils import cfg

def start_bronze_stream(spark):
    eh_conn_string = dbutils.secrets.get(
        scope="realtime-secrets",
        key="eh-connection-string"
    )

    jaas = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{eh_conn_string}\";'

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka["bootstrap_servers"])
        .option("subscribe", cfg.kafka["topic"])
        .option("startingOffsets", cfg.kafka['starting_offsets']) 
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "60000")
        .option("kafka.group.id", "bronze-stream")
        .option("failOnDataLoss", "false")
        .load()
    )

    # -------------------- BRONZE TRANSFORMATION --------------------
    bronze_df = (
        kafka_df.select(
            col("value").cast("string").alias("raw_value"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            current_timestamp().alias("ingestion_time")
        )
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
        .withColumn("ingestion_hour", hour(col("ingestion_time")))
    )

    # -------------------- WRITE BRONZE DELTA --------------------
    query = (
        bronze_df.writeStream
        .trigger(processingTime=cfg.streaming['trigger_interval'])
        .format("delta")
        .queryName("Ingestion_EventHub_to_Bronze")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoints['bronze'])
        .partitionBy("ingestion_date", "ingestion_hour")
        .start(cfg.paths['bronze'])
    )
    # def log_batch_metrics(batch_df, batch_id):
    #     print(f"\n========== BATCH {batch_id} ==========")
    #     print(f"Records in this batch: {batch_df.count()}")
    #     print("=====================================\n")

    # query = (
    #     bronze_df.writeStream
    #     .foreachBatch(log_batch_metrics)
    #     .trigger(processingTime=cfg.streaming['trigger_interval'])
    #     .start(checkpointLocation="abfss://checkpoints@realtimelog.dfs.core.windows.net/bronze")
    # )

    return query


if __name__ == "__main__":
    spark = get_spark("Kafka_to_Bronze")
    query = start_bronze_stream(spark)
    query.awaitTermination()

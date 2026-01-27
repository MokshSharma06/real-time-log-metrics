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
        .option("startingOffsets", "latest") 
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas)
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "60000")
        .option("kafka.group.id", "bronze-stream")
        .load()
    )

    # -------------------- BRONZE TRANSFORMATION --------------------
    # (Kept exactly as you had it - it's a solid raw-ingestion pattern)
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
        .format("delta")
        .queryName("Ingestion_EventHub_to_Bronze")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoints['bronze'])
        .partitionBy("ingestion_date", "ingestion_hour")
        .start(cfg.paths['bronze'])
    )

    return query


if __name__ == "__main__":
    query = start_bronze_stream(spark)
    query.awaitTermination()

# src/bronze/kafka_to_bronze.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, hour
from src.schema import LOG_SCHEMA
from src.utils import get_spark_session
import yaml
from src.utils import load_config

def main():
    # Load config
    config = load_config()
    kafka_conf = config["kafka"]
    base_paths = config["paths"]

    bronze_path = f"{base_paths['bronze']}/logs"
    checkpoint_path = f"{base_paths['checkpoints']['bronze']}/logs"


    spark = get_spark_session()

    

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_conf["bootstrap_servers"])
        .option("subscribe", kafka_conf["topic"])
        .option("startingOffsets", "latest")
        .load()
    )

    # Bronze transformation (RAW + METADATA ONLY)
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

    # Write Bronze Delta
    (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("ingestion_date", "ingestion_hour")
        .start(bronze_path)
        .awaitTermination()
    )
    
    # spark.read.format("delta") \
    # .load("data/bronze/logs") \
    # .show(100000,truncate=100)


if __name__ == "__main__":
    main()
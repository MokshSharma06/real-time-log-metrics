# src/bronze/kafka_to_bronze.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, hour
from src.schema import LOG_SCHEMA
from src.utils import get_spark_session
import yaml
from src.utils import cfg

from pyspark.sql.functions import col, current_timestamp, to_date, hour


def start_bronze_stream(spark):
    bronze_path=cfg.paths['bronze']

    # -------------------- READ FROM KAFKA --------------------
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka["bootstrap_servers"])
        .option("subscribe", cfg.kafka["topic"])
        .option("startingOffsets",cfg.kafka['starting_offsets'])
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
        .format("delta")
        .queryName("Ingestion Kafka to Bronze")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoints['bronze'])
        .partitionBy("ingestion_date", "ingestion_hour")
        .start(bronze_path)
    )

    return query


# if __name__ == "__main__":
#     from pyspark.sql import SparkSession
    
#     spark = SparkSession.builder.appName("BronzeLayerTest").getOrCreate()
    
#     print(f"Starting Bronze Stream in {cfg.env} mode...")
#     query = start_bronze_stream(spark)
#     query.awaitTermination()
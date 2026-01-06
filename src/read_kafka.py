from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from src.schema import LOG_SCHEMA
from src.utils import get_spark_session


spark=get_spark_session()


raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "app-logs") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), LOG_SCHEMA).alias("data")
).select("data.*")


query = parsed_df.writeStream \
    .format("console") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

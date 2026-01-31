import pytest
from pyspark.sql.functions import from_json, col
from pyspark.sql import Row
import datetime
from src.schema import LOG_SCHEMA
from src.utils import get_spark_session
from src.silver.bronze_to_silver import multi_sink_writer
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

@pytest.fixture(scope="session")
def spark():
    spark = get_spark_session()
    yield spark
    spark.stop()

clean_path = "data/silver/clean"
bad_path = "data/silver/bad"
late_path = "data/silver/late"

def test_idempotency(spark):
    sample_data = [
        Row(
            event_id="5db0cb23-4f25-48a", 
            service_name="payment", 
            status_code= 200, 
            event_time=datetime.datetime(2026, 1, 16, 15, 0), 
            ingestion_time=datetime.datetime(2026, 1, 16, 15, 0),
            kafka_partition=2,
            kafka_offset=169,
            is_valid=True,
            is_late=False 
        )
    ]
    test_df = spark.createDataFrame(sample_data) \
        .withColumn("status_code", F.col("status_code").cast(IntegerType())) \
        .withColumn("kafka_partition", F.col("kafka_partition").cast(IntegerType()))
    
    print("first ingestion")
    multi_sink_writer(test_df, 1)
    
    first_count = spark.read.format("delta").load(clean_path).count()
    print(f"Count after first run: {first_count}")

    print("second ingestion with same data")
    multi_sink_writer(test_df, 2)
    
    second_count = spark.read.format("delta").load(clean_path).count()
    print(f"Count after second run: {second_count}")

    if first_count == second_count:
        print("TEST PASSED: Pipeline is Idempotent!")
    else:
        print("TEST FAILED: Duplicates detected!")
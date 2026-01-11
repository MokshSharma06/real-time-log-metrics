import pytest
from pyspark.sql.functions import from_json, col
from src.schema import LOG_SCHEMA
from src.utils import get_spark_session

@pytest.fixture(scope="session")
def spark():
    spark = get_spark_session()
    yield spark
    spark.stop()


def test_schema_parsing(spark):
    data = '{"event_id": "e1", "event_time": "2026-01-11 10:00:00", "service": "auth", "status_code": 200}'
    df = spark.createDataFrame([(data,)], ["value"])
    parsed_df = df.withColumn("data", from_json(col("value"), LOG_SCHEMA)).select("data.*")
    
    row = parsed_df.collect()[0]
    assert row["event_id"] == "e1"
    assert row["service"] == "auth"
    assert row["status_code"] == 200
    assert hasattr(row["event_time"], 'year')

def test_missing_fields(spark):
    data = '{"event_id": "e2"}'
    df = spark.createDataFrame([(data,)], ["value"])

    parsed_df = df.withColumn("data", from_json(col("value"), LOG_SCHEMA)).select("data.*")
    row = parsed_df.collect()[0]
    
    assert row["event_id"] == "e2"
    assert row["service"] is None
    assert row["status_code"] is None
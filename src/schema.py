from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

LOG_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("service", StringType(), nullable=False),
    StructField("status_code", IntegerType(), nullable=False)
])

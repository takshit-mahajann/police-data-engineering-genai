from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)


POLICE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("date_time", TimestampType(), True),
    StructField("type", StringType(), True),
    StructField("subtype", StringType(), True),
    StructField("location", StringType(), True),
    StructField("description", StringType(), True),
    StructField("last_updated", TimestampType(), True),

    # API metadata
    StructField(":id", StringType(), True),
    StructField(":version", StringType(), True),
    StructField(":created_at", TimestampType(), True),
    StructField(":updated_at", TimestampType(), True)
])
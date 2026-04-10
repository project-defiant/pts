"""PySpark schema for TEP (Therapeutic European Programme) data."""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# TEP schema (tep.json.gz)
tep_schema: StructType = StructType([
    StructField('targetFromSourceId', StringType(), True),
    StructField('description', StringType(), True),
    StructField('therapeuticArea', StringType(), True),
    StructField('url', StringType(), True),
])

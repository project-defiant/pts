"""PySpark schema for safety liability data."""

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

# Safety evidence raw schema
safety_evidence_raw_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('targetFromSourceId', StringType(), True),
    StructField('event', StringType(), True),
    StructField('eventId', StringType(), True),
    StructField('effects', StringType(), True),
    StructField('biosamples', StringType(), True),
    StructField('datasource', StringType(), True),
    StructField('literature', StringType(), True),
    StructField('url', StringType(), True),
    StructField('studies', StringType(), True),
])

# Biosample structure
biosample_struct: StructType = StructType([
    StructField('tissueLabel', StringType(), True),
    StructField('tissueId', StringType(), True),
    StructField('cellLabel', StringType(), True),
    StructField('cellFormat', StringType(), True),
])

# Target safety study structure
target_safety_study_struct: StructType = StructType([
    StructField('name', StringType(), True),
    StructField('description', StringType(), True),
    StructField('type', StringType(), True),
])

# Target safety evidence structure
target_safety_evidence_struct: StructType = StructType([
    StructField('event', StringType(), True),
    StructField('eventId', StringType(), True),
    StructField('effects', ArrayType(ArrayType(StringType())), True),
    StructField('biosamples', ArrayType(biosample_struct), True),
    StructField('datasource', StringType(), True),
    StructField('literature', StringType(), True),
    StructField('url', StringType(), True),
    StructField('studies', ArrayType(target_safety_study_struct), True),
])

# Target safety schema (after processing)
target_safety_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('safetyLiabilities', ArrayType(target_safety_evidence_struct), True),
])

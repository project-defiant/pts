"""PySpark schema for HPA (Human Protein Atlas) data."""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# HPA subcellular location schema (subcellular_location.tsv.zip)
subcellular_location_schema: StructType = StructType([
    StructField('ensembl_id', StringType(), True),
    StructField('location', StringType(), True),
    StructField('status', StringType(), True),
    StructField('antibody', StringType(), True),
    StructField('main_location', StringType(), True),
    StructField('supporting_location', StringType(), True),
    StructField('additional_location', StringType(), True),
    StructField('validation', StringType(), True),
    StructField('location_id', StringType(), True),
])

# HPA SSL schema (subcellular_locations_ssl.parquet)
# This is a lookup table for location synonyms
hpa_ssl_schema: StructType = StructType([
    StructField('HPA_location', StringType(), True),
    StructField('termSL', StringType(), True),
    StructField('labelSL', StringType(), True),
])

# Final subcellular locations schema after processing
subcellular_locations_schema: StructType = StructType([
    StructField('location', StringType(), True),
    StructField('source', StringType(), True),
    StructField('termSL', StringType(), True),
    StructField('labelSL', StringType(), True),
])

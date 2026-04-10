"""PySpark schema for Gene Code data."""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# Gene Code GFF3 schema (gencode.gff3.gz)
# This is a GFF3 format file with specific columns
gene_code_schema: StructType = StructType([
    StructField('seqname', StringType(), True),
    StructField('source', StringType(), True),
    StructField('feature', StringType(), True),
    StructField('start', StringType(), True),
    StructField('end', StringType(), True),
    StructField('score', StringType(), True),
    StructField('strand', StringType(), True),
    StructField('frame', StringType(), True),
    StructField('attributes', StringType(), True),
])

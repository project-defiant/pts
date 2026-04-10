"""PySpark schema for hallmarks data."""

from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Hallmarks schema (cosmic-hallmarks.tsv)
hallmarks_raw_schema: StructType = StructType([
    StructField('gene_symbol', StringType(), True),
    StructField('pmid', LongType(), True),
    StructField('hallmark', StringType(), True),
    StructField('impact', StringType(), True),
    StructField('description', StringType(), True),
])

# Hallmark structure
hallmark_struct: StructType = StructType([
    StructField('pmid', LongType(), True),
    StructField('description', StringType(), True),
    StructField('name', StringType(), True),
])

# Cancer hallmark structure
cancer_hallmark_struct: StructType = StructType([
    StructField('pmid', LongType(), True),
    StructField('description', StringType(), True),
    StructField('impact', StringType(), True),
    StructField('label', StringType(), True),
])

# Hallmarks with id (after processing)
hallmarks_schema: StructType = StructType([
    StructField('approvedSymbol', StringType(), True),
    StructField(
        'hallmarks',
        StructType([
            StructField('attributes', ArrayType(hallmark_struct), True),
            StructField('cancerHallmarks', ArrayType(cancer_hallmark_struct), True),
        ]),
        True,
    ),
])

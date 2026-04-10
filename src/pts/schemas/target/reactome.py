"""PySpark schema for Reactome data."""

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

# Reactome pathways schema (Ensembl2Reactome.txt)
reactome_pathways_schema: StructType = StructType([
    StructField('ensemblId', StringType(), True),
    StructField('reactomeId', StringType(), True),
    StructField('url', StringType(), True),
    StructField('eventName', StringType(), True),
    StructField('eventCode', StringType(), True),
    StructField('species', StringType(), True),
])

# Reactome ETL schema (from intermediate/reactome)
reactome_etl_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('label', StringType(), True),
    StructField('path', ArrayType(ArrayType(StringType())), True),
])

# Reactomes with id (after processing)
reactomes_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField(
        'pathways',
        ArrayType(
            StructType([
                StructField('pathwayId', StringType(), True),
                StructField('pathway', StringType(), True),
                StructField('topLevelTerm', StringType(), True),
            ])
        ),
        True,
    ),
])

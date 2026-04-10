"""PySpark schema for genetic constraint data (gnomad)."""

from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Genetic constraints schema (gnomad_constraint_metrics.tsv)
constraint_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField(
        'constraint',
        ArrayType(
            StructType([
                StructField('constraintType', StringType(), True),
                StructField('score', FloatType(), True),
                StructField('exp', FloatType(), True),
                StructField('obs', IntegerType(), True),
                StructField('oe', FloatType(), True),
                StructField('oeLower', FloatType(), True),
                StructField('oeUpper', FloatType(), True),
                StructField('upperRank', IntegerType(), True),
                StructField('upperBin', IntegerType(), True),
                StructField('upperBin6', IntegerType(), True),
            ])
        ),
        True,
    ),
])

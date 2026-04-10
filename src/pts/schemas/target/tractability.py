"""PySpark schema for tractability data."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)

# Tractability schema (tractability.tsv)
tractability_raw_schema: StructType = StructType([
    StructField('ensembl_gene_id', StringType(), True),
    # Dynamic columns matching pattern _B\d+_ (e.g., SM_B1_Approved_drugs)
])

# Tractability structure
tractability_struct: StructType = StructType([
    StructField('modality', StringType(), True),
    StructField('id', StringType(), True),
    StructField('value', BooleanType(), True),
])

# Tractability with id (after processing)
tractability_schema: StructType = StructType([
    StructField('ensemblGeneId', StringType(), True),
    StructField('tractability', ArrayType(tractability_struct), True),
])

"""PySpark schema for project scores data."""

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

# Project scores gene identifiers schema
project_scores_ids_schema: StructType = StructType([
    StructField('gene_id', StringType(), True),
    StructField('cosmic_gene_symbol', StringType(), True),
    StructField('ensembl_gene_id', StringType(), True),
    StructField('entrez_id', StringType(), True),
    StructField('hgnc_id', StringType(), True),
    StructField('hgnc_symbol', StringType(), True),
    StructField('refseq_id', StringType(), True),
    StructField('uniprot_id', StringType(), True),
])

# Project scores dependency matrix schema (wide format with cell lines as columns)
project_scores_matrix_schema: StructType = StructType([
    StructField('Gene', StringType(), True),
    # Dynamic columns for each cell line (e.g., MCF7, A549, etc.)
])

# Gene with dbXRef structure
id_and_source_struct: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('source', StringType(), True),
])

# Gene with dbXRef schema (after processing)
gene_with_dbxref_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('xRef', ArrayType(id_and_source_struct), True),
])

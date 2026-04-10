"""PySpark schema for UniProt data."""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# UniProt text file schema (uniprot.txt.gz)
# Based on the UniProt SPTAG format and the Scala Uniprot converter
schema: StructType = StructType([
    StructField('entry', StringType(), True),
    StructField('entry_name', StringType(), True),
    StructField('status', StringType(), True),
    StructField('protein_name', StringType(), True),
    StructField('gene_names', StringType(), True),
    StructField('organism', StringType(), True),
    StructField('ncbi_tax_id', StringType(), True),
    StructField('seq_length', StringType(), True),
    StructField('seq_md5', StringType(), True),
    StructField('ensembl_gene_id', StringType(), True),
    StructField('ensembl_transcript_id', StringType(), True),
    StructField('ensembl_protein_id', StringType(), True),
    StructField('refseq_id', StringType(), True),
    StructField('gene_symbol', StringType(), True),
    StructField('hgnc_id', StringType(), True),
    StructField('omim_id', StringType(), True),
    StructField('db_resource', StringType(), True),
])

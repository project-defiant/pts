"""PySpark schema for gene ontology data."""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# Gene ontology evidence codes (GOA Human GAF format)
goa_human_gaf_schema: StructType = StructType([
    StructField('database', StringType(), True),
    StructField('database_object_id', StringType(), True),
    StructField('database_object_symbol', StringType(), True),
    StructField('qualifier', StringType(), True),
    StructField('go_id', StringType(), True),
    StructField('database_reference', StringType(), True),
    StructField('evidence_code', StringType(), True),
    StructField('with_or_from', StringType(), True),
    StructField('aspect', StringType(), True),
    StructField('database_object_name', StringType(), True),
    StructField('synonym', StringType(), True),
    StructField('database_object_type', StringType(), True),
    StructField('taxon', StringType(), True),
    StructField('date', StringType(), True),
    StructField('assigned_by', StringType(), True),
    StructField('annotation_extension', StringType(), True),
    StructField('gene_product_form_id', StringType(), True),
])

# Ensembl to GO schema
ensembl_go_schema: StructType = StructType([
    StructField('ensembl_gene_id', StringType(), True),
    StructField('go_id', StringType(), True),
    StructField('evidence_code', StringType(), True),
    StructField('go_aspect', StringType(), True),
])

# GO structure for target
go_struct: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('source', StringType(), True),
    StructField('evidence', StringType(), True),
    StructField('aspect', StringType(), True),
    StructField('geneProduct', StringType(), True),
    StructField('ecoId', StringType(), True),
])

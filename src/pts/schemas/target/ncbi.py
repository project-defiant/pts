"""PySpark schema for NCBI data."""

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

# NCBI schema (Homo_sapiens.gene_info)
ncbi_raw_schema: StructType = StructType([
    StructField('tax_id', StringType(), True),
    StructField('GeneID', StringType(), True),
    StructField('Symbol', StringType(), True),
    StructField('Synonyms', StringType(), True),
    StructField('dbXrefs', StringType(), True),
    StructField('chromosome', StringType(), True),
    StructField('map_location', StringType(), True),
    StructField('description', StringType(), True),
    StructField('type_of_gene', StringType(), True),
    StructField('Symbol_from_nomenclature_authority', StringType(), True),
    StructField('Full_name_from_nomenclature_authority', StringType(), True),
    StructField('Nomenclature_status', StringType(), True),
    StructField('Other_designations', StringType(), True),
    StructField('Modification_date', StringType(), True),
])

# NCBI with processed synonyms (after processing)
ncbi_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField(
        'synonyms',
        ArrayType(
            StructType([
                StructField('label', StringType(), True),
                StructField('source', StringType(), True),
            ])
        ),
        True,
    ),
    StructField(
        'symbolSynonyms',
        ArrayType(
            StructType([
                StructField('label', StringType(), True),
                StructField('source', StringType(), True),
            ])
        ),
        True,
    ),
    StructField(
        'nameSynonyms',
        ArrayType(
            StructType([
                StructField('label', StringType(), True),
                StructField('source', StringType(), True),
            ])
        ),
        True,
    ),
])

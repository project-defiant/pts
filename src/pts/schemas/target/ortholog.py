"""PySpark schema for ortholog/homology data."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Homology species dictionary schema (species_EnsemblVertebrates.txt)
species_dict_schema: StructType = StructType([
    StructField('name', StringType(), True),
    StructField('species', StringType(), True),
    StructField('taxonomy_id', StringType(), True),
])

# Homology coding proteins schema (protein-*.tsv.gz and ncrna-*.tsv.gz)
# Based on Ensembl homology TSV format
homology_schema: StructType = StructType([
    StructField('gene_stable_id', StringType(), True),
    StructField('protein_stable_id', StringType(), True),
    StructField('species', StringType(), True),
    StructField('identity', StringType(), True),
    StructField('homology_type', StringType(), True),
    StructField('homology_gene_stable_id', StringType(), True),
    StructField('homology_protein_stable_id', StringType(), True),
    StructField('homology_species', StringType(), True),
    StructField('homology_identity', StringType(), True),
    StructField('dn', StringType(), True),
    StructField('ds', StringType(), True),
    StructField('goc_score', StringType(), True),
    StructField('wga_coverage', StringType(), True),
    StructField('is_high_confidence', StringType(), True),
    StructField('homology_id', StringType(), True),
])

# Homology gene dictionary schema (gene_dictionary/*.json)
homology_gene_dict_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
])

# Final ortholog schema after processing
ortholog_schema: StructType = StructType([
    StructField('id', StringType(), True),
    StructField('speciesId', StringType(), True),
    StructField('speciesName', StringType(), True),
    StructField('homologyType', StringType(), True),
    StructField('targetGeneId', StringType(), True),
    StructField('isHighConfidence', StringType(), True),
    StructField('targetGeneSymbol', StringType(), True),
    StructField('queryPercentageIdentity', DoubleType(), True),
    StructField('targetPercentageIdentity', DoubleType(), True),
    StructField('priority', IntegerType(), True),
])

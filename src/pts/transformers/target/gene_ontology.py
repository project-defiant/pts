"""PySpark loaders for gene ontology data."""

from pyspark.sql import DataFrame, SparkSession


def load_gene_ontology(spark: SparkSession, path: str) -> DataFrame:
    """Load gene ontology lookup from parquet file."""
    return spark.read.parquet(path)

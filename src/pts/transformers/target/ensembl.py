"""PySpark loaders for Ensembl data."""

from pyspark.sql import DataFrame, SparkSession


def load_ensembl(spark: SparkSession, path: str) -> DataFrame:
    """Load Ensembl data from parquet file."""
    return spark.read.parquet(path)

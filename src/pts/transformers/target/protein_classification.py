"""PySpark loaders for protein classification data."""

from pyspark.sql import DataFrame, SparkSession


def load_protein_classification(spark: SparkSession, path: str) -> DataFrame:
    """Load protein classification data from parquet file."""
    return spark.read.parquet(path)

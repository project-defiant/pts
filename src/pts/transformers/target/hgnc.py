"""PySpark loaders for HGNC data."""

from pyspark.sql import DataFrame, SparkSession


def load_hgnc(spark: SparkSession, path: str) -> DataFrame:
    """Load HGNC data from JSON file."""
    return spark.read.json(path)

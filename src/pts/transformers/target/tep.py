"""PySpark loaders for TEP data."""

from pyspark.sql import DataFrame, SparkSession


def load_tep(spark: SparkSession, path: str) -> DataFrame:
    """Load TEP data from gzipped JSON file."""
    return spark.read.json(path)

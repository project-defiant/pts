"""PySpark loaders for safety liability data."""

from pyspark.sql import DataFrame, SparkSession


def load_safety(spark: SparkSession, path: str) -> DataFrame:
    """Load safety liability data from parquet files."""
    return spark.read.parquet(path)

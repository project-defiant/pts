"""PySpark loaders for hallmarks data."""

from pyspark.sql import DataFrame, SparkSession


def load_hallmarks(spark: SparkSession, path: str) -> DataFrame:
    """Load hallmarks data from TSV file."""
    return spark.read.csv(path, sep='\t', header=True)

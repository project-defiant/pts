"""PySpark loaders for tractability data."""

from pyspark.sql import DataFrame, SparkSession


def load_tractability(spark: SparkSession, path: str) -> DataFrame:
    """Load tractability data from TSV file."""
    return spark.read.csv(path, sep='\t', header=True)

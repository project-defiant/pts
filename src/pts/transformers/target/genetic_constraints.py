"""PySpark loaders for genetic constraints data."""

from pyspark.sql import DataFrame, SparkSession


def load_genetic_constraints(spark: SparkSession, path: str) -> DataFrame:
    """Load genetic constraints data from TSV file."""
    return spark.read.csv(path, sep='\t', header=True)

"""PySpark loaders for NCBI data."""

from pyspark.sql import DataFrame, SparkSession


def load_ncbi(spark: SparkSession, path: str) -> DataFrame:
    """Load NCBI gene info from gzipped TSV file."""
    return spark.read.csv(path, sep='\t', header=True, mode='PERMISSIVE')

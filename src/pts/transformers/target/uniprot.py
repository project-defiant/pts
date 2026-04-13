"""PySpark loaders for UniProt data."""

from pyspark.sql import DataFrame, SparkSession


def load_uniprot(spark: SparkSession, path: str) -> DataFrame:
    """Load UniProt data from text file."""
    return spark.read.csv(path, sep='\t', header=False)


def load_uniprot_ssl(spark: SparkSession, path: str) -> DataFrame:
    """Load UniProt SSL data from TSV file."""
    return spark.read.csv(path, sep='\t', header=True)

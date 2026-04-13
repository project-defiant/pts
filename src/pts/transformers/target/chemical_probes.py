"""PySpark loaders for chemical probes data."""

from pyspark.sql import DataFrame, SparkSession


def load_chemical_probes(spark: SparkSession, path: str) -> DataFrame:
    """Load chemical probes data from Excel file."""
    return spark.read.format('com.crealytics.spark.excel').option('header', 'true').load(path)

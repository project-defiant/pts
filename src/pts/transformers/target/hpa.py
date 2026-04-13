"""PySpark loaders for HPA (Human Protein Atlas) data."""

from pyspark.sql import DataFrame, SparkSession


def load_hpa(spark: SparkSession, path: str) -> DataFrame:
    """Load HPA data (subcellular location + SSL).

    Args:
        spark: Spark session.
        path: Path to HPA parquet file containing SSL data.

    Returns:
        DataFrame with HPA subcellular location data.
    """
    return spark.read.parquet(path)

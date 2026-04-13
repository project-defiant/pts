"""PySpark loaders for Gene Code data."""

from pyspark.sql import DataFrame, SparkSession


def load_gene_code(spark: SparkSession, path: str) -> DataFrame:
    """Load Gene Code GFF3 data from gzipped file."""
    # GFF3 is a tab-separated format with comments starting with #
    return spark.read.csv(path, sep='\t', header=False)

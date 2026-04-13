"""PySpark loaders for project scores data."""

from pyspark.sql import DataFrame, SparkSession


def load_project_scores_ids(spark: SparkSession, path: str) -> DataFrame:
    """Load project scores gene identifiers from parquet file."""
    return spark.read.parquet(path)


def load_project_scores_matrix(spark: SparkSession, path: str) -> DataFrame:
    """Load project scores dependency matrix from parquet file."""
    return spark.read.parquet(path)


def load_essentiality(spark: SparkSession, path: str) -> DataFrame:
    """Load gene essentiality data from parquet file."""
    return spark.read.parquet(path)


def load_project_scores(
    spark: SparkSession,
    ids_path: str,
    matrix_path: str,
) -> DataFrame:
    """Load project scores from multiple sources.

    Args:
        spark: Spark session.
        ids_path: Path to project scores IDs parquet file.
        matrix_path: Path to project scores matrix parquet file.

    Returns:
        Combined DataFrame with project scores data.
    """
    ids_df = load_project_scores_ids(spark, ids_path)
    matrix_df = load_project_scores_matrix(spark, matrix_path)
    return ids_df.unionByName(matrix_df)

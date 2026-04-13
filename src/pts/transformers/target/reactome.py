"""PySpark loaders for Reactome data."""

from pyspark.sql import DataFrame, SparkSession


def load_reactome_pathways(spark: SparkSession, path: str) -> DataFrame:
    """Load Reactome pathways from TSV file."""
    return spark.read.csv(path, sep='\t', header=False)


def load_reactome_etl(spark: SparkSession, path: str) -> DataFrame:
    """Load Reactome ETL data from parquet files."""
    return spark.read.parquet(path)


def load_reactome(
    spark: SparkSession,
    etl_path_glob: str,
    pathways_path: str,
) -> DataFrame:
    """Load Reactome data from multiple sources.

    Args:
        spark: Spark session.
        etl_path_glob: Path to Reactome ETL parquet files.
        pathways_path: Path to Reactome pathways TSV file.

    Returns:
        Combined DataFrame with Reactome data.
    """
    etl_df = load_reactome_etl(spark, etl_path_glob)
    pathways_df = load_reactome_pathways(spark, pathways_path)
    return etl_df.unionByName(pathways_df)

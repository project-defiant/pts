"""PySpark loaders for ortholog/homology data."""

from pyspark.sql import DataFrame, SparkSession


def load_ortholog_dict(spark: SparkSession, path: str) -> DataFrame:
    """Load ortholog species dictionary from text file."""
    return spark.read.csv(path, sep='\t', header=False)


def load_coding_proteins(spark: SparkSession, path: str) -> DataFrame:
    """Load coding proteins homology data from TSV files."""
    return spark.read.csv(path, sep='\t', header=False)


def load_homology_gene_dict(spark: SparkSession, path: str) -> DataFrame:
    """Load homology gene dictionary from JSON files."""
    return spark.read.json(path)


def load_ortholog(
    spark: SparkSession,
    dict_path: str,
    coding_proteins_glob: str,
    gene_dict_glob: str,
) -> DataFrame:
    """Load ortholog data from multiple sources.

    Args:
        spark: Spark session.
        dict_path: Path to species dictionary file.
        coding_proteins_glob: Glob pattern for coding proteins files.
        gene_dict_glob: Glob pattern for gene dictionary files.

    Returns:
        DataFrame with ortholog data.
    """
    dict_df = load_ortholog_dict(spark, dict_path)
    coding_df = load_coding_proteins(spark, coding_proteins_glob)
    # gene_dict_df = load_homology_gene_dict(spark, gene_dict_glob)

    # Return combined DataFrame for testing
    return dict_df.unionByName(coding_df)

"""PTS target essentiality step - PySpark implementation.

This module implements the target_essentiality dataset generation.
It adds gene essentiality data to the target dataset.

The gene essentiality data is preprocessed by the gene_essentiality step.
"""

from typing import Any

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from pts.pyspark.common.session import Session


def target_essentiality(
    source: dict[str, str],
    destination: str,
    settings: dict[str, Any],
    properties: dict[str, str],
) -> None:
    """Compute the target_essentiality dataset.

    Args:
        source: Dictionary with paths to:
            - essentiality: Gene essentiality parquet from gene_essentiality step
            - target: Target parquet from target step
        destination: Path to write the output parquet file.
        settings: Custom settings (not used).
        properties: Spark configuration options.
    """
    spark = Session(app_name='target_essentiality', properties=properties)

    logger.info(f'Loading data from {source}')

    # Load input data
    essentiality_df = spark.load_data(source['essentiality'])
    target_df = spark.load_data(source['target'])

    logger.info('Processing target essentiality data')

    # Add gene essentiality to targets
    result_df = _add_gene_essentiality(target_df, essentiality_df)

    logger.info(f'Writing output data to {destination}')

    # Coalesce to single partition as per original implementation
    result_df.coalesce(1).write.parquet(destination, mode='overwrite')


def _add_gene_essentiality(target_df: DataFrame, essentiality_df: DataFrame) -> DataFrame:
    """Add gene essentiality data to target DataFrame.

    Args:
        target_df: Target DataFrame with id column (Ensembl gene IDs).
        essentiality_df: Gene essentiality DataFrame with targetSymbol column.

    Returns:
        DataFrame with essentiality data joined to targets.
    """
    # Get essential genes from the essentiality data
    essential_genes = (
        essentiality_df
        .filter(f.col('isEssential'))
        .select(
            f.col('targetSymbol').alias('approvedSymbol'),
            f.lit(True).alias('isEssential'),
        )
        .distinct()
    )

    # Join with target data
    return target_df.join(essential_genes, on='approvedSymbol', how='left')

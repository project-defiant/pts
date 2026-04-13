from typing import Any

import polars as pl
from loguru import logger
from otter.config.model import Config
from otter.storage.synchronous.handle import StorageHandle


def interaction(
    source: str,
    destination: str,
    settings: dict[str, Any],
    config: Config,
) -> None:
    logger.info('Loading interaction inputs')
    h = StorageHandle(source)
    f = h.open()

    import json

    with f:
        data = json.load(f)

    logger.info('Parsing interaction data')

    graphs = data.get('graphs', [])
    if not graphs:
        raise ValueError('No graphs found in interaction data')

    nodes = []
    edges = []

    for graph in graphs:
        nodes.extend(graph.get('nodes', []))
        edges.extend(graph.get('edges', []))

    logger.info(f'Loaded {len(nodes)} nodes and {len(edges)} edges')

    node_df = pl.DataFrame(nodes)
    edge_df = pl.DataFrame(edges)

    node_df = node_df.with_columns(
        pl.col('id').str.split('/').list.last().alias('node_id'),
        pl.col('lbl').alias('label'),
    )

    edge_df = edge_df.with_columns(
        pl.col('sub').str.split('/').list.last().alias('src'),
        pl.col('obj').str.split('/').list.last().alias('dst'),
    )

    if 'propertyValues' in edge_df.columns:
        edge_df = edge_df.with_columns(
            pl.col('propertyValues').list.first().struct.field('value').alias('score'),
        )
    else:
        edge_df = edge_df.with_columns(pl.lit(1.0).alias('score'))

    edge_df = edge_df.select('src', 'dst', 'score')

    output_df = edge_df

    logger.info(f'Output has {output_df.height} edges')
    logger.info('Writing interaction output')

    d = StorageHandle(destination)
    output_df.write_parquet(d.absolute, compression='gzip')

    logger.info('Interaction transformation complete')

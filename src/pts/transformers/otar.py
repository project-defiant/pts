from typing import Any

import polars as pl
from loguru import logger
from otter.config.model import Config
from otter.storage.synchronous.handle import StorageHandle


def otar(
    source: str,
    destination: str,
    settings: dict[str, Any],
    config: Config,
) -> None:
    logger.info('Loading OTAR inputs')
    h = StorageHandle(source)
    f = h.open()

    import json

    with f:
        data = json.load(f)

    graph = data.get('graphs', [{}])[0]
    nodes = graph.get('nodes', [])
    edges = graph.get('edges', [])

    logger.info(f'Loaded {len(nodes)} nodes and {len(edges)} edges')

    node_df = pl.DataFrame(nodes)
    edge_df = pl.DataFrame(edges)

    logger.info('Processing OTAR data')

    node_df = node_df.with_columns(pl.col('id').str.split('/').list.last().alias('otar_code'))
    edge_df = edge_df.with_columns(
        pl.col('sub').str.split('/').list.last().alias('otar_code'),
        pl.col('obj').str.split('/').list.first().alias('efo_disease_id'),
    )

    merged = node_df.join(edge_df, on='otar_code', how='left')
    merged = merged.with_columns(pl.col('obj').str.split('/').list.last().alias('efo_disease_id'))
    merged = merged.with_columns(
        pl
        .when(pl.col('efo_disease_id').is_null())
        .then(pl.col('obj'))
        .otherwise(pl.col('efo_disease_id'))
        .alias('efo_disease_id')
    )

    projects_df = merged.group_by('otar_code').agg(
        pl.first('id').alias('project_name'),
        pl.first('name').alias('project_name'),
        pl.first('project_status').alias('project_status'),
        pl.first('integrates_in_PPP').cast(pl.Boolean).alias('integrates_data_PPP'),
        pl.col('efo_disease_id').unique().alias('efo_disease_id'),
    )

    projects_df = projects_df.with_columns(
        pl
        .when(pl.col('integrates_data_PPP').is_null())
        .then(pl.lit(False))
        .otherwise(pl.col('integrates_data_PPP'))
        .alias('integrates_data_PPP')
    )

    projects_df = projects_df.with_columns(
        pl.struct({
            'otar_code': pl.col('otar_code'),
            'status': pl.col('project_status'),
            'project_name': pl.col('project_name'),
            'integrates_data_PPP': pl.col('integrates_data_PPP'),
            'reference': pl.format('http://home.opentargets.org/{}', pl.col('otar_code')),
        }).alias('project')
    )

    output_df = projects_df.group_by('efo_disease_id').agg(pl.col('project').alias('projects'))

    logger.info(f'Output has {output_df.height} rows')
    logger.info('Writing OTAR output')

    d = StorageHandle(destination)
    output_df.write_parquet(d.absolute, compression='gzip')

    logger.info('OTAR transformation complete')

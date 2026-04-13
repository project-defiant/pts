from typing import Any

import polars as pl
from loguru import logger
from otter.config.model import Config
from otter.storage.synchronous.handle import StorageHandle


def reactome(source: str, destination: str, settings: dict[str, Any], config: Config) -> None:
    logger.info('Loading Reactome inputs')
    h = StorageHandle(source)
    f = h.open()

    import json

    with f:
        data = json.load(f)

    graphs = data.get('graphs', [])
    if not graphs:
        raise ValueError('No graphs found in Reactome data')

    graph = graphs[0]
    nodes = graph.get('nodes', [])
    edges = graph.get('edges', [])

    logger.info(f'Loaded {len(nodes)} nodes and {len(edges)} edges')

    node_df = pl.DataFrame(nodes)
    edge_df = pl.DataFrame(edges)

    node_df = node_df.with_columns(pl.col('id').str.split('/').list.last().alias('node_id'))
    edge_df = edge_df.with_columns(
        pl.col('sub').str.split('/').list.last().alias('src'),
        pl.col('obj').str.split('/').list.last().alias('dst'),
    )

    node_df = node_df.filter(pl.col('type') == 'Pathway')
    node_df = node_df.select('node_id', 'lbl').with_columns(pl.col('node_id').alias('id'), pl.col('lbl').alias('name'))

    edge_df = edge_df.select('src', 'dst')

    logger.info('Building graph structure')

    from pts.transformers.utils.helpers import union_dataframes_different_schema, flatten_concat

    parent_df = edge_df.group_by('src').agg(pl.col('dst').alias('parents'))

    child_df = edge_df.group_by('dst').agg(pl.col('src').alias('children'))

    node_with_parents = node_df.join(parent_df, on='id', how='left')
    node_with_children = node_with_parents.join(child_df, on='id', how='left')

    node_with_children = node_with_children.with_columns(
        pl.col('parents').fill_null(pl.lit([])),
        pl.col('children').fill_null(pl.lit([])),
    )

    node_with_children = node_with_children.with_columns(
        pl.col('parents').list.unique(),
        pl.col('children').list.unique(),
    )

    df = node_with_children

    ancestors_list = []
    descendants_list = []

    for row in df.iter_rows(named=True):
        id_ = row['id']
        parents = row.get('parents', []) or []
        children = row.get('children', []) or []

        ancestors = set(parents)
        descendants = set(children)

        current = list(parents)
        while current:
            next_parent = current.pop(0)
            if next_parent not in ancestors:
                ancestors.add(next_parent)
                parent_row = node_with_children.filter(pl.col('id') == next_parent)
                if not parent_row.is_empty():
                    parent_parents = parent_row[0].get('parents', []) or []
                    current.extend(parent_parents)

        current = list(children)
        while current:
            next_child = current.pop(0)
            if next_child not in descendants:
                descendants.add(next_child)
                child_row = node_with_children.filter(pl.col('id') == next_child)
                if not child_row.is_empty():
                    child_children = child_row[0].get('children', []) or []
                    current.extend(child_children)

        ancestors_list.append(list(ancestors))
        descendants_list.append(list(descendants))

    df = df.with_columns(
        pl.Series('ancestors', ancestors_list),
        pl.Series('descendants', descendants_list),
    )

    df = df.with_columns(
        pl.col('id').alias('id'),
        pl.col('name').alias('name'),
        pl.col('ancestors').alias('ancestors'),
        pl.col('descendants').alias('descendants'),
        pl.col('children').alias('children'),
        pl.col('parents').alias('parents'),
        pl.col('id').list.prepend(pl.col('id')).alias('path'),
    )

    logger.info(f'Output has {df.height} rows')
    logger.info('Writing Reactome output')

    d = StorageHandle(destination)
    df.write_parquet(d.absolute, compression='gzip')

    logger.info('Reactome transformation complete')

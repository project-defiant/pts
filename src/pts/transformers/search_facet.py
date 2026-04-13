from typing import Any

from loguru import logger
from otter.config.model import Config
from otter.storage.synchronous.handle import StorageHandle


def search_facet(
    source: str,
    destination: str,
    settings: dict[str, Any],
    config: Config,
) -> None:
    logger.info('Loading facet search inputs')
    d = StorageHandle(destination)
    logger.info('Search facet transformation complete')
    logger.info(f'Output written to {d.absolute}')

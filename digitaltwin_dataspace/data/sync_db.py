from functools import lru_cache, partial
from typing import Dict, List, Callable

from sqlalchemy import Table, MetaData, inspect
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from .engine import engine
from .table import load_simple_table_from_configuration
from ..components import Component


def get_or_create_table_with_provider(
        table_name: str,
        table_provider: Callable[[MetaData], Table]
) -> Table:
    """
    Low-level function to get or create a table using a column provider.

    :param table_provider:
    :param table_name: Table name
    :param table_provider: Function that provides a Table object given a MetaData instance
    :return: SQLAlchemy Table object
    """
    metadata = MetaData()
    inspector = inspect(engine)

    if table_name not in inspector.get_table_names():
        table = table_provider(metadata)
        try:
            metadata.create_all(engine)
        except OperationalError:
            pass
    else:
        table = Table(table_name, metadata, autoload_with=engine)

    return table


@lru_cache
def get_or_create_standard_component_table(table_name: str) -> Table:
    """
    Get or create a standard component table using the global provider registry.

    :param table_name: Table name
    :return: SQLAlchemy Table object
    """
    return get_or_create_table_with_provider(
        table_name=table_name,
        table_provider=partial(load_simple_table_from_configuration, table_name)
    )


def sync_db_from_configuration(
        components: List[Component],
) -> Dict[str, Table]:
    """
    Sync the database from the components' configuration.
    :param components: The components to sync
    :return: The tables
    """

    metadata_obj = MetaData()

    tables = {}
    indexes = []
    for component in components:
        config = component.get_configuration()
        tables[config.name] = load_simple_table_from_configuration(
            config.name, metadata_obj
        )
        # create index on
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS {component.name}_date_index ON {name} (date)"
        )

    metadata_obj.create_all(engine, checkfirst=True)

    with engine.connect() as connection:
        for index in indexes:
            connection.execute(text(index))
        connection.commit()

    return tables

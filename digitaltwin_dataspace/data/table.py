from sqlalchemy import (
    Column,
    TIMESTAMP,
    INTEGER,
    Table,
    MetaData,
    VARCHAR,
    Index
)


def load_simple_table_from_configuration(table_name: str, metadata_obj: MetaData):
    """
    Load/Create a simple table from a component configuration.

    A simple table is a table that contains an id, a date, a data column, a type column, a hash column, and a copy_id column.
    The copy_id column is used to prevent storing the same data multiple times, instead, it stores the id of the row that contains the same data,
    leveraging the unique constraint on the hash column.

    @param table_name: The table name
    @param metadata_obj: The metadata object
    @return: The table
    """
    return Table(
        table_name,
        metadata_obj,
        Column("id", INTEGER, primary_key=True, autoincrement=True),
        Column("date", TIMESTAMP, nullable=False),
        Column("data", VARCHAR(512), nullable=True),
        Column("type", VARCHAR(24), nullable=True),
        Column("hash", VARCHAR(32), nullable=True),
        Column("copy_id", INTEGER, nullable=True),
        Index(f"${table_name}_date_index", "date"),
    )

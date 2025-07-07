import json
from dataclasses import dataclass
from datetime import datetime
from typing import Union, List, Optional

from sqlalchemy import Table, select
from sqlalchemy.orm import aliased
from sqlalchemy.sql.functions import coalesce

from .engine import engine
from .storage import storage_manager


@dataclass
class Data:
    date: datetime
    hash: str
    _url: str
    content_type: str = None
    description: str = None
    @property
    def data(self) -> bytes:
        return storage_manager.read(self._url)


def data_result(func) -> Optional[Union[Data, List[Data]]]:
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if result is None:
            return None
        # If the result is a single row, return a single Data object
        if not isinstance(result, list):
            return Data(date=result.date, _url=result.data, content_type=result.type, hash=result.hash, description=result.description)

        return [
            Data(date=row.date, _url=row.data, content_type=row.type, hash=row.hash, description=row.description)
            for row in result
        ]

    return wrapper


def base_query(table: Table, with_null: bool = False):
    """
    Returns a base query for a table. But replace the value of the date column
    when it is None with the value of the row with the id matching the copy_id.
    :param table: The table
    :param with_null: Whether to include rows with null data
    :return: The base query to use for all subsequent queries
    """

    t2 = aliased(table)

    query = select(
        table.c.id,
        table.c.date,
        coalesce(t2.c.data, table.c.data).label("data"),
        table.c.type,
        coalesce(t2.c.hash, table.c.hash).label("hash"),
        coalesce(t2.c.description, table.c.description).label("description"),
    )

    if not with_null:
        query = query.where((table.c.copy_id.isnot(None)) | (table.c.hash.isnot(None)))

    query = query.select_from(table).outerjoin(t2, table.c.copy_id == t2.c.id)

    return query


@data_result
def retrieve_latest_row(table: Table, with_null: bool = False) -> Data:
    """
    Get the latest row from a table.
    :param table: The table
    :param with_null: Whether to include rows with null data
    :return: The latest row
    """
    with engine.connect() as connection:
        return connection.execute(
            base_query(table, with_null=with_null)
            .order_by(table.c.date.desc())
            .limit(1)
        ).fetchone()


@data_result
def retrieve_first_row(table: Table) -> Data:
    """
    Get the first row from a table.
    :param table: The table
    :return: The first row
    """
    with engine.connect() as connection:
        return connection.execute(
            base_query(table).order_by(table.c.date.asc()).limit(1)
        ).fetchone()


@data_result
def retrieve_after_datetime(table: Table, date: datetime, limit: int) -> List[Data]:
    with engine.connect() as connection:
        return connection.execute(
            base_query(table)
            .where(table.c.date > date)
            .order_by(table.c.date.desc())
            .limit(limit)
        ).fetchall()


@data_result
def retrieve_before_datetime(table: Table, date: datetime, limit: int) -> List[Data]:
    with engine.connect() as connection:
        return connection.execute(
            base_query(table)
            .where(table.c.date < date)
            .order_by(table.c.date.desc())
            .limit(limit)
        ).fetchall()


@data_result
def retrieve_between_datetime(
    table: Table, start_date: datetime, end_date: datetime, limit: int
) -> List[Data]:
    with engine.connect() as connection:
        if start_date is None:
            return connection.execute(
                base_query(table)
                .where(table.c.date < end_date)
                .order_by(table.c.date.asc())
                .limit(limit)
            ).fetchall()
        elif end_date is None:
            return connection.execute(
                base_query(table)
                .where(table.c.date > start_date)
                .order_by(table.c.date.asc())
                .limit(limit)
            ).fetchall()
        else:
            return connection.execute(
                base_query(table)
                .where(table.c.date > start_date)
                .where(table.c.date < end_date)
                .order_by(table.c.date.asc())
                .limit(limit)
            ).fetchall()


@data_result
def retrieve_latest_rows_before_datetime(
    table: Table, date: datetime, limit: int
) -> List[Data]:
    with engine.connect() as connection:
        return connection.execute(
            base_query(table)
            .where(table.c.date < date)
            .order_by(table.c.date.desc())
            .limit(limit)
        ).fetchall()


@data_result
def retrieve_latest_row_before_datetime(
    table: Table, date: datetime, with_null: bool = False
) -> Optional[Data]:
    with engine.connect() as connection:
        return connection.execute(
            base_query(table, with_null=with_null)
            .where(table.c.date < date)
            .order_by(table.c.date.desc())
            .limit(1)
        ).fetchone()

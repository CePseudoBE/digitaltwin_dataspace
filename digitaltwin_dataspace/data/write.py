import hashlib
import json
from datetime import datetime

from sqlalchemy import Table

from .engine import engine
from .storage import storage_manager


def write_result(
        name: str, content_type: str, table: Table, data, date: datetime
):
    """
    Write the result of a harvester to the database.
    If the data already exists, it will be overwritten.
    :param name:  The name of the folder to write to in the storage
    :param content_type:  The content type of the data (e.g., "text", "json", etc.)
    :param table:  The table to write to
    :param data:  The data to write
    :param date:  The date of the data
    """

    if data is str:
        data_bytes = data.encode("utf-8")
    elif isinstance(data, dict) or isinstance(data, list):
        data_bytes = json.dumps(data).encode("utf-8")
    else:
        data_bytes = data

    if data_bytes is None:
        md5_digest = None
    else:
        md5_digest = hashlib.md5(data_bytes).hexdigest()

    with engine.connect() as connection:

        # Upload data to storage
        url = storage_manager.write(
            f"{name}/{date.strftime('%Y-%m-%d_%H-%M-%S')}",
            data_bytes,
        )
        # Insert data to database
        connection.execute(
            table.insert().values(
                date=date, data=url, hash=md5_digest, type=content_type
            )
        )

        connection.commit()

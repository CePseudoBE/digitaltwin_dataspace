import hashlib
import json
from datetime import datetime

from sqlalchemy import Table

from .engine import engine
from .storage import storage_manager


def write_result(
        name: str, content_type: str, table: Table, data, date: datetime, 
        description: str = None, append_path: str = None
):
    """
    Write the result of a harvester to the database.
    If the data already exists, it will be overwritten.
    :param name:  The name of the folder to write to in the storage
    :param content_type:  The content type of the data (e.g., "text", "json", etc.)
    :param table:  The table to write to
    :param data:  The data to write
    :param date:  The date of the data
    :param description:  The description of the data
    :param append_path:  The path to append to the data
    """
    if isinstance(data, str):
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
            f"{name}/{date.strftime('%Y-%m-%d_%H-%M-%S')}/{append_path}",
            data_bytes,
        )
        # Insert data to database
        connection.execute(
            table.insert().values(
                date=date, data=url, hash=md5_digest, type=content_type, description=description
            )
        )

        connection.commit()

def delete_result(table: Table, url: str):
    with engine.connect() as connection:
        connection.execute(
            table.delete().where(table.c.data == url)
        )
        connection.commit()
        storage_manager.delete(url)


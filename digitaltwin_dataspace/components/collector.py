import abc
from datetime import datetime
from typing import Any

from fastapi import Response
import json
from .base import Component, ScheduleRunnable, Servable, servable_endpoint
from ..data.retrieve import retrieve_latest_row_before_datetime, retrieve_before_datetime
from ..data.sync_db import get_or_create_standard_component_table
from ..data.write import write_result, delete_result


class Collector(Component, ScheduleRunnable, Servable, abc.ABC):

    def get_table(self):
        return get_or_create_standard_component_table(self.get_configuration().name)

    @servable_endpoint(path="/")
    def retrieve(self, timestamp: datetime = None) -> Response:
        data = retrieve_latest_row_before_datetime(
            self.get_table(),
            timestamp if timestamp else datetime.now(),
        )
        return Response(content=data.data, media_type=data.content_type)
    
    @servable_endpoint(path="/all")
    def retrieve_all(self) -> Response:
        data = retrieve_before_datetime(
            self.get_table(),
            datetime.now(),
            limit=1000
        )
        assets = []
        for data in data:
            if json.loads(data.data).get("layer") is not None:
                r = json.loads(data.data)["layer"]
                r["_url"] = data._url
                assets.append(r)
        return Response(content=json.dumps(assets), media_type='application/json')
    
    @servable_endpoint(path="/delete", method="DELETE", response_model=str)
    def delete(self, url: str):
        delete_result(self.get_table(), url)
        return f"Deleted file {url}"
    
    def run(self) -> Any:
        result = self.collect()

        if result is not None:
            config = self.get_configuration()
            write_result(config.name, config.content_type, self.get_table(), result, datetime.now())

        return result

    @abc.abstractmethod
    def collect(self) -> bytes:
        """
        Overrides the `collect` method to retrieve content from a distant data provider.
        """
        pass

    

import abc
from datetime import datetime
from typing import Any

from fastapi import Response, UploadFile, Form

from .base import Component, Servable, servable_endpoint
from ..data.retrieve import retrieve_before_datetime
from ..data.sync_db import get_or_create_standard_component_table
from ..data.write import write_result, delete_result, write_tileset
from ..utilities.zip_to_dict import zip_to_dict
import json

class AssetsManager(Component, Servable, abc.ABC):

    def get_table(self):
        return get_or_create_standard_component_table(self.get_configuration().name)

    @servable_endpoint(path="/")
    def get_assets(self, timestamp: datetime = None) -> Response:
        data = retrieve_before_datetime(
            self.get_table(),
            timestamp if timestamp else datetime.now(),
            limit=1000
        )
        assets = [{'url': data._url, 'description' : data.description} for data in data]
        return Response(content=json.dumps(assets))

    @servable_endpoint(path="/upload", method="POST", response_model=str)
    def upload(self, file: UploadFile, description: str = Form(...)):
        config = self.get_configuration()
        write_result(config.name, config.content_type, self.get_table(), file.file.read(), datetime.now(), append_path=file.filename, description=description)
        return f"Received file and processed via collect."
    
    @servable_endpoint(path="/delete", method="DELETE", response_model=str)
    def delete(self, url: str):
        delete_result(self.get_table(), url)
        return f"Deleted file {url}"
    

class TilesetManager(AssetsManager):
    @servable_endpoint(path="/upload", method="POST", response_model=str)
    def upload(self, zip_file: UploadFile, description: str = Form(...)):
        print("uploading tileset")
        folder_json = zip_to_dict(zip_file)
        print("folder_json", folder_json.keys())
        write_tileset(folder_json, self.get_configuration().name, self.get_configuration().content_type, self.get_table(), datetime.now(), description=description)
        return f"Received item with name: {folder_json.get('name', 'N/A')} and processed via collect."
    
    @servable_endpoint(path="/")
    def retrieve(self, timestamp: datetime = None) -> Response:
        data = retrieve_before_datetime(
            self.get_table(),
            timestamp if timestamp else datetime.now(),
            limit=1000
        )
        data = [{"url": item._url, "description": item.description} for item in data if "tileset.json" in item._url]
        return Response(content=json.dumps(data))   

    @servable_endpoint(path="/delete", method="DELETE", response_model=str)
    def delete(self, url: str):
        delete_tileset(self.get_table(), url)
        return f"Deleted file {url}"
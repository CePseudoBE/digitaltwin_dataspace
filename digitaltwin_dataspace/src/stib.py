import dotenv
import json
from typing import List, Dict
import geopandas as gpd
import pandas as pd
import requests
import shapely
from bs4 import BeautifulSoup
from collections import defaultdict
from itertools import chain, product

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components
from components.stib.utils.fetch import auth_request_to_stib, fetch_stib_dataset_records
from components.stib.utils.constant import VEHICLE_POSITION_DATASET
from components.stib.utils.converter import convert_dataframe_column_stop_to_generic

class STIBGTFSCollector(Collector):
    def get_schedule(self) -> str:
        return "1h"  # Mise à jour horaire
    
    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_gtfs_collector",
            tags=["STIB", "GTFS"],
            description="Collecte les données GTFS statiques de la STIB",
            content_type="application/zip"
        )
    
    def collect(self) -> bytes:
        response = auth_request_to_stib(
            "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/gtfs-files-production/alternative_exports/gtfszip/"
        )
        return response.content

class STIBShapeFilesCollector(Collector):
    def get_schedule(self) -> str:
        return "1h"
    
    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_shapefiles_collector",
            tags=["STIB", "GeoJSON"],
            description="Collecte les shapefiles du réseau STIB",
            content_type="application/geo+json"
        )
    
    def collect(self) -> bytes:
        response = auth_request_to_stib(
            "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/shapefiles-production/exports/geojson"
        )
        return response.content

class STIBStopsCollector(Collector):
    def get_schedule(self) -> str:
        return "24h"  # Mise à jour quotidienne
    
    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_stops_collector",
            tags=["STIB", "GeoJSON", "Arrêts"],
            description="Collecte et fusionne les données d'arrêts officielles et non officielles",
            content_type="application/geo+json"
        )
    
    def collect(self) -> bytes:
        data = self.merge_unofficial_and_official_stops_data()
        with_lat_and_long = data[data["stop_lat"].notnull() & data["stop_lon"].notnull()]
        
        gdf = gpd.GeoDataFrame(
            with_lat_and_long,
            crs="epsg:4326",
            geometry=[
                shapely.geometry.Point(xy)
                for xy in zip(with_lat_and_long["stop_lon"], with_lat_and_long["stop_lat"])
            ]
        )
        
        return json.dumps(json.loads(gdf.to_json())).encode('utf-8')

    # Méthodes utilitaires conservées avec adaptation mineure
    @staticmethod
    def merge_unofficial_and_official_stops_data():
        # Implémentation existante conservée
        ...

class STIBVehiclePositionsCollector(Collector):
    def get_schedule(self) -> str:
        return "30s"  # Mise à jour temps réel
    
    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_vehicle_positions_collector",
            tags=["STIB", "Temps réel"],
            description="Collecte les positions des véhicules en temps réel",
            content_type="application/json"
        )
    
    def collect(self) -> bytes:
        raw_results = fetch_stib_dataset_records(
            dataset=VEHICLE_POSITION_DATASET,
            limit=100
        )
        
        results = []
        for raw_result in map(lambda x: x["fields"], raw_results):
            for vehicle_position in json.loads(raw_result["vehiclepositions"]):
                results.append({**vehicle_position, "lineId": str(raw_result["lineid"])})
        
        return json.dumps(results).encode('utf-8')

run_components([
    STIBGTFSCollector(),
    STIBShapeFilesCollector(),
    STIBStopsCollector(),
    STIBVehiclePositionsCollector()
])

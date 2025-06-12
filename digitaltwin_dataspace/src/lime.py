import json
import geopandas as gpd
import pandas as pd
import shapely
import requests
import dotenv

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class LimeVehiclePositionCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="lime_vehicle_position_collector",
            tags=["Lime", "Vehicle", "Position"],
            description="Collecte les positions des véhicules Lime à Bruxelles",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://data.lime.bike/api/partners/v2/gbfs/brussels/free_bike_status"
        response_json = requests.get(endpoint).json()
        response_df = pd.json_normalize(response_json["data"]["bikes"])
        response_gdf = gpd.GeoDataFrame(
            response_df,
            crs="epsg:4326",
            geometry=[
                shapely.geometry.Point(xy)
                for xy in zip(response_df["lon"], response_df["lat"])
            ],
        )
        response_gdf = response_gdf.drop(columns=["lat", "lon"])
        return response_gdf.to_json().encode('utf-8')

class LimeVehicleTypeCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="lime_vehicle_type_collector",
            tags=["Lime", "Vehicle", "Type"],
            description="Collecte les types de véhicules Lime à Bruxelles",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://data.lime.bike/api/partners/v2/gbfs/brussels/vehicle_types"
        response = requests.get(endpoint)
        return response.content

run_components([
    LimeVehiclePositionCollector(),
    LimeVehicleTypeCollector()
])

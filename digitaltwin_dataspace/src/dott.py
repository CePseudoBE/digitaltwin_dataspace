import json
import geopandas as gpd
import pandas as pd
import shapely
import requests

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class DottGeofenceCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="dott_geofence_collector",
            tags=["Dott", "Geofence"],
            description="Collecte les zones de géorepérage Dott à Bruxelles",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://gbfs.api.ridedott.com/public/v2/brussels/geofencing_zones.json"
        response = requests.get(endpoint)
        return response.content

class DottVehiclePositionCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="dott_vehicle_position_collector",
            tags=["Dott", "Vehicle", "Position"],
            description="Collecte les positions des véhicules Dott à Bruxelles",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://gbfs.api.ridedott.com/public/v2/brussels/free_bike_status.json"
        response = requests.get(endpoint)
        response_json = response.json()
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

class DottVehicleTypeCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="dott_vehicle_type_collector",
            tags=["Dott", "Vehicle", "Type"],
            description="Collecte les types de véhicules Dott à Bruxelles",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://gbfs.api.ridedott.com/public/v2/brussels/vehicle_types.json"
        response = requests.get(endpoint)
        return response.content

run_components([
    DottGeofenceCollector(),
    DottVehiclePositionCollector(),
    DottVehicleTypeCollector()
])

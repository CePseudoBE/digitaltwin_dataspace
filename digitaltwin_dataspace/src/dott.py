import json
import geopandas as gpd
import pandas as pd
import shapely
import requests
import dotenv

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class DottGeofenceCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les 10 minutes

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
        response.raise_for_status()
        bikes = response.json()["data"]["bikes"]

        features = []
        for bike in bikes:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [bike["lon"], bike["lat"]],
                },
                "properties": {
                    k: v for k, v in bike.items()
                    if k not in ["lat", "lon"]
                }
            }
            features.append(feature)

        geojson = {
            "type": "FeatureCollection",
            "features": features
        }

        return json.dumps(geojson).encode("utf-8")

class DottVehicleTypeCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les 10 minutes

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

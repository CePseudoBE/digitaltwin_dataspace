import json
import geopandas as gpd
import pandas as pd
import shapely
import requests
from requests import JSONDecodeError
import dotenv

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class PonyGeofenceCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="pony_geofence_collector",
            tags=["Pony", "Geofence"],
            description="Collecte les zones de géorepérage Pony à Bruxelles",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://gbfs.getapony.com/v1/Brussels/en/geofencing_zones.json"
        response = requests.get(endpoint)
        return response.content

class PonyVehiclePositionCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="pony_vehicle_position_collector",
            tags=["Pony", "Vehicle", "Position"],
            description="Collecte les positions des véhicules Pony à Bruxelles",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://gbfs.getapony.com/v1/Brussels/en/free_bike_status.json"
        response = requests.get(endpoint)

        try:
            response_json = response.json()
            bikes = response_json["data"]["bikes"]

            features = []
            for bike in bikes:
                # Construire manuellement le GeoJSON
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [bike["lon"], bike["lat"]]
                    },
                    "properties": {
                        k: v for k, v in bike.items()
                        if k not in ["lat", "lon", "rental_uris"]
                    }
                }

                # Ajouter rental_uris s’il existe
                if "rental_uris" in bike:
                    feature["properties"]["rental_uris"] = bike["rental_uris"]

                features.append(feature)

            geojson = {
                "type": "FeatureCollection",
                "features": features
            }

            return json.dumps(geojson).encode("utf-8")

        except JSONDecodeError:
            raise Exception("Pony API is not available, returned: " + response.text)

class PonyVehicleTypeCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="pony_vehicle_type_collector",
            tags=["Pony", "Vehicle", "Type"],
            description="Collecte les types de véhicules Pony à Bruxelles",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://gbfs.getapony.com/v1/Brussels/en/vehicle_types.json"
        response = requests.get(endpoint)
        return response.content

run_components([
    PonyGeofenceCollector(),
    PonyVehiclePositionCollector(),
    PonyVehicleTypeCollector()
])

import json
import geopandas as gpd
import pandas as pd
import shapely
import requests
import dotenv

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class BoltGeofenceCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="bolt_geofence_collector",
            tags=["Bolt", "Geofence"],
            description="Collecte les zones de géorepérage Bolt à Bruxelles",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://mds.bolt.eu/gbfs/2/336/geofencing_zones"
        response = requests.get(endpoint)
        return response.content

class BoltVehiclePositionCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="bolt_vehicle_position_collector",
            tags=["Bolt", "Vehicle", "Position"],
            description="Collecte les positions des véhicules Bolt à Bruxelles",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://mds.bolt.eu/gbfs/2/336/free_bike_status"
        response_json = requests.get(endpoint).json()
        bikes = response_json["data"]["bikes"]

        features = []
        for bike in bikes:
            # Construire le GeoJSON manuellement
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

            # Ajouter rental_uris proprement
            if "rental_uris" in bike:
                feature["properties"]["rental_uris"] = bike["rental_uris"]

            features.append(feature)

        geojson = {
            "type": "FeatureCollection",
            "features": features
        }

        return json.dumps(geojson).encode("utf-8")



class BoltVehicleTypeCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="bolt_vehicle_type_collector",
            tags=["Bolt", "Vehicle", "Type"],
            description="Collecte les types de véhicules Bolt à Bruxelles",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://mds.bolt.eu/gbfs/2/336/vehicle_types"
        response = requests.get(endpoint)
        return response.content

run_components([
    BoltGeofenceCollector(),
    BoltVehiclePositionCollector(),
    BoltVehicleTypeCollector()
])

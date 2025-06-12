import dotenv
dotenv.load_dotenv()

import os
import json
import requests
import geopandas as gpd
import pandas as pd
from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

api_key = os.getenv("STIB_API_KEY")

url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/stops-production/records?limit=1"
headers = {
    "Authorization": f"Bearer {api_key}"
}

response = requests.get(url, headers=headers)

print("Status code:", response.status_code)
print("Response:", response.text)

class STIBGTFSCollector(Collector):
    def get_schedule(self) -> str:
        return "30s"  # Collecte horaire suffisante pour GTFS statique

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_gtfs_collector",
            tags=["STIB", "GTFS"],
            description="Collecte les données GTFS statiques de la STIB",
            content_type="application/zip"
        )

    def collect(self) -> bytes:
        url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/gtfs-files-production/alternative_exports/gtfszip/"
        response = requests.get(url)
        response.raise_for_status()
        return response.content

class STIBShapeFilesCollector(Collector):
    def get_schedule(self) -> str:
        return "30s"  # Mise à jour quotidienne des shapefiles

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_shapefiles_collector",
            tags=["STIB", "GeoJSON"],
            description="Collecte les shapefiles du réseau STIB",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/shapefiles-production/exports/geojson"
        response = requests.get(url)
        response.raise_for_status()
        return response.content

class STIBStopsCollector(Collector):
    def get_schedule(self) -> str:
        return "30s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_stops_collector",
            tags=["STIB", "GeoJSON", "Arrêts"],
            description="Collecte les arrêts STIB",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/stops-production/records"
        params = {
            "where": "stop_lat is not null",
            "limit": 1000,
            "refine":"location_type:0"  # Filtre pour les arrêts physiques
        }
        
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {os.environ['STIB_API_KEY']}"},
            params=params
        )
        
        # Vérification avancée de la réponse
        if response.status_code == 401:
            raise ValueError("Clé API STIB invalide ou expirée")
            
        response.raise_for_status()
        
        data = response.json()
        if not data.get("results"):
            raise ValueError("Aucun arrêt trouvé dans la réponse API")

        df = pd.json_normalize(data["results"])
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df['stop_lon'], df['stop_lat']),
            crs="EPSG:4326"
        )
        
        return gdf.to_json().encode("utf-8")

class STIBVehiclePositionsCollector(Collector):
    def get_schedule(self) -> str:
        return "30s"  # Mise à jour toutes les 30 secondes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_vehicle_positions_collector",
            tags=["STIB", "Temps réel"],
            description="Collecte les positions des véhicules STIB",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/vehicle-position-production/records"
        
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {os.environ['STIB_API_KEY']}"},
            params={"limit": 250}  # Augmentation de la limite
        )
        
        if response.status_code == 401:
            raise PermissionError("Accès refusé - Vérifiez la clé API")
            
        response.raise_for_status()
        
        raw_data = response.json()
        if not raw_data.get("results"):
            return json.dumps({"type":"FeatureCollection","features":[]}).encode()

        features = []
        for entry in raw_data["results"]:
            try:
                vehicles = json.loads(entry["vehiclepositions"])
                for vehicle in vehicles:
                    if all(k in vehicle for k in ["longitude", "latitude"]):
                        features.append({
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [vehicle["longitude"], vehicle["latitude"]]
                            },
                            "properties": {
                                "lineId": entry["lineid"],
                                "vehicleId": vehicle.get("vehicleid"),
                                "speed": vehicle.get("speed"),
                                "timestamp": vehicle.get("timestamp")
                            }
                        })
            except json.JSONDecodeError:
                continue

        return json.dumps({"type":"FeatureCollection","features": features}).encode()

run_components([
    STIBGTFSCollector(),
    STIBShapeFilesCollector(),
    STIBStopsCollector(),
    STIBVehiclePositionsCollector()
])

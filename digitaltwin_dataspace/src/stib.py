import json
import requests
import geopandas as gpd
import pandas as pd
import shapely
from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components
import os
import dotenv

dotenv.load_dotenv()
print("DATABASE_URL =", repr(os.environ.get("DATABASE_URL")))

class STIBGTFSCollector(Collector):
    def get_schedule(self) -> str:
        return "1h"

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
        print("DATABASE_URL =", os.environ.get("DATABASE_URL"))
        response.raise_for_status()
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
        url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/shapefiles-production/exports/geojson"
        response = requests.get(url)
        print("DATABASE_URL =", os.environ.get("DATABASE_URL"))
        response.raise_for_status()
        return response.content


class STIBStopsCollector(Collector):
    def get_schedule(self) -> str:
        return "24h"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_stops_collector",
            tags=["STIB", "GeoJSON", "Arrêts"],
            description="Collecte les arrêts STIB (exemple simplifié)",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        # Dataset des arrêts
        url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/stops-production/records?limit=1000"
        response = requests.get(url)
        response.raise_for_status()
        records = response.json().get("results", [])

        df = pd.DataFrame(records)
        df = df[df["stop_lat"].notnull() & df["stop_lon"].notnull()]

        gdf = gpd.GeoDataFrame(
            df,
            crs="EPSG:4326",
            geometry=gpd.points_from_xy(df["stop_lon"], df["stop_lat"])
        )

        return gdf.to_json().encode("utf-8")


class STIBVehiclePositionsCollector(Collector):
    def get_schedule(self) -> str:
        return "30s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_vehicle_positions_collector",
            tags=["STIB", "Temps réel"],
            description="Collecte les positions des véhicules STIB",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        url = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/vehicle-positions-production/records?limit=100"
        response = requests.get(url)
        response.raise_for_status()
        raw_data = response.json().get("results", [])

        features = []
        for entry in raw_data:
            if "vehiclepositions" in entry and "lineid" in entry:
                vehicle_data = json.loads(entry["vehiclepositions"])
                for v in vehicle_data:
                    lon = v.get("longitude")
                    lat = v.get("latitude")
                    if lon is not None and lat is not None:
                        feature = {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [lon, lat]},
                            "properties": {**v, "lineId": entry["lineid"]},
                        }
                        features.append(feature)

        geojson = {"type": "FeatureCollection", "features": features}
        return json.dumps(geojson).encode("utf-8")


run_components([
    STIBGTFSCollector(),
    STIBShapeFilesCollector(),
    STIBStopsCollector(),
    STIBVehiclePositionsCollector()
])

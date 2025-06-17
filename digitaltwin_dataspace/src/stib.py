import json
import dotenv
from collections import defaultdict
from itertools import chain, product
from typing import Dict, List

import geopandas as gpd
import pandas as pd
import requests
import shapely
from bs4 import BeautifulSoup

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration


class STIBGTFSCollector(Collector):
    def get_schedule(self) -> str:
        return "1h"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_gtfs_collector",
            tags=["STIB", "GTFS", "Transport"],
            description="Collecte les données GTFS statiques de la STIB pour Bruxelles",
            content_type="application/zip"
        )

    def collect(self) -> bytes:
        endpoint = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/gtfs-files-production/alternative_exports/gtfszip/"
        response = requests.get(endpoint, timeout=10)
        response.raise_for_status()
        return response.content


class STIBShapeFilesCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_shapefiles_collector",
            tags=["STIB", "GeoJSON", "Shapefiles"],
            description="Collecte les shapefiles du réseau STIB en format GeoJSON",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/shapefiles-production/exports/geojson"
        response = requests.get(endpoint, timeout=10)
        response.raise_for_status()
        return response.content


class STIBVehiclePositionsCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_vehicle_positions_collector",
            tags=["STIB", "Vehicle", "Positions", "Real-time"],
            description="Collecte les positions des véhicules STIB en temps réel",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/vehicle-position-rt-production/records"

        try:
            response = requests.get(endpoint, params={"limit": 100}, timeout=10)
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"Erreur lors de la récupération des positions de véhicules : {e}")
            return json.dumps([]).encode('utf-8')

        raw_results = data.get("results", [])
        results = []

        for raw_result in raw_results:
            fields = raw_result.get("fields", {})
            try:
                vehicle_positions = json.loads(fields.get("vehiclepositions", "[]"))
                for vp in vehicle_positions:
                    results.append({**vp, "lineId": str(fields.get("lineid", ""))})
            except json.JSONDecodeError:
                continue

        return json.dumps(results).encode('utf-8')


class STIBStopsCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="stib_stops_collector",
            tags=["STIB", "Arrêts", "GeoJSON"],
            description="Collecte les arrêts STIB avec coordonnées géographiques",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        try:
            stops_per_line = pd.read_csv(
                "https://stibmivb.opendatasoft.com/explore/dataset/gtfs-files-production/files"
                "/7068c8d492df76c5125fac081b5e09e9/download/"
            )
        except Exception as e:
            print(f"Erreur lors du chargement des arrêts officiels : {e}")
            stops_per_line = pd.DataFrame(columns=["stop_id", "stop_lat", "stop_lon"])

        stops_per_line["stop_id"] = self._convert_stop_ids_to_generic(stops_per_line["stop_id"])
        stops_per_line = stops_per_line[["stop_id", "stop_lat", "stop_lon"]]

        unofficial_stops = self._fetch_stops_by_scraping()

        merged = unofficial_stops.merge(stops_per_line, on=["stop_id"], how="left")
        merged = merged.drop_duplicates(subset=["stop_id", "route_short_name", "direction_id"], keep="first")
        with_coords = merged[merged["stop_lat"].notnull() & merged["stop_lon"].notnull()]

        if with_coords.empty:
            return json.dumps({
                "type": "FeatureCollection",
                "features": []
            }).encode('utf-8')

        gdf = gpd.GeoDataFrame(
            with_coords,
            crs="epsg:4326",
            geometry=[shapely.geometry.Point(lon, lat) for lon, lat in zip(with_coords["stop_lon"], with_coords["stop_lat"])]
        )

        return gdf.to_json().encode('utf-8')

    def _fetch_stops_by_scraping(self) -> pd.DataFrame:
        stops_data = []
        direction_choice = ("V", "F")
        noctis = ["N04", "N05", "N06", "N08", "N09", "N10", "N11", "N12", "N13", "N16", "N18"]

        for line, direction in product(chain(range(1, 100), noctis), direction_choice):
            try:
                url = (
                    f"https://www.stib-mivb.be/irj/servlet/prt/portal/prtroot/"
                    f"pcd!3aportal_content!2fSTIBMIVB!2fWebsite!2fFrontend!2fPublic!2f"
                    f"iViews!2fcom.stib.HorairesServletService"
                    f"?l=fr&_line={line}&_directioncode={direction}&_mode=rt"
                )

                response = requests.get(url, timeout=10)
                if response.status_code != 200:
                    continue

                soup = BeautifulSoup(response.content, "html.parser")
                li_elements = soup.find_all("li", class_="thermometer__stop")

                for sequence, li in enumerate(li_elements):
                    stop_id = li.get("id")
                    stop_name = li.text.replace("\n", "").strip()

                    if stop_id and stop_name:
                        stops_data.append({
                            "route_short_name": str(line).replace("T", ""),
                            "direction_id": direction,
                            "direction": 0 if direction == "V" else 1,
                            "stop_id": stop_id,
                            "stop_name": stop_name,
                            "stop_sequence": sequence,
                        })

            except Exception as e:
                print(f"Erreur scraping ligne {line} direction {direction}: {e}")
                continue

        df = pd.DataFrame(stops_data)
        if not df.empty:
            df["stop_id"] = self._convert_stop_ids_to_generic(df["stop_id"])

        return df

    def _convert_stop_ids_to_generic(self, stop_ids):
        return stop_ids.astype(str).str.replace(r'[^0-9]', '', regex=True)


run_components([
    STIBGTFSCollector(),
    STIBShapeFilesCollector(),
    STIBVehiclePositionsCollector(),
    STIBStopsCollector(),
])

import dotenv
import requests

dotenv.load_dotenv()

import pandas as pd
import geopandas as gpd
from pyproj import Transformer
import json

from digitaltwin_dataspace import run_components, Collector, Harvester, ComponentConfiguration, \
    HarvesterConfiguration, Data


class FixMyStreetIncidentsCollector(Collector):
    def get_schedule(self) -> str:
        return "1h"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="fixmystreet_collector",
            tags=["FixMyStreet", "Brussels"],
            description="Collects incident data from FixMyStreet Brussels",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        transformer = Transformer.from_crs("EPSG:31370", "EPSG:4326", always_xy=True)

        response = requests.get("https://fixmystreet.brussels/api/incidents?page=0&size=12")
        response.raise_for_status()
        data = response.json()
        incidents = data["_embedded"]["response"]

        df = pd.json_normalize(incidents, sep="_")

        # Nettoyer les NaN (indispensable pour json.dumps)
        df = df.where(pd.notnull(df), None)

        # Convertir en coordonnées géographiques
        x = df["location_coordinates_x"].astype(float)
        y = df["location_coordinates_y"].astype(float)
        lon, lat = transformer.transform(x.values, y.values)
        geometries = gpd.points_from_xy(lon, lat)

        # Supprimer les colonnes inutiles
        cols_to_drop = [col for col in df.columns if col.startswith("_links")] + [
            "location_coordinates_x",
            "location_coordinates_y",
        ]
        df.drop(columns=cols_to_drop, inplace=True, errors="ignore")

        # Re-nesting des propriétés
        def nest_props(flat_dict):
            nested = {}
            for key, value in flat_dict.items():
                parts = key.split("_")
                current = nested
                for i, part in enumerate(parts):
                    if i == len(parts) - 1:
                        current[part] = value
                    else:
                        current = current.setdefault(part, {})
            return nested

        # Construire la FeatureCollection
        features = []
        for i, row in df.iterrows():
            nested_props = nest_props(row.to_dict())
            geometry = geometries[i]
            features.append({
                "type": "Feature",
                "properties": nested_props,
                "geometry": {
                    "type": "Point",
                    "coordinates": [geometry.x, geometry.y],
                },
                "id": str(i),
            })

        return json.dumps({
            "type": "FeatureCollection",
            "features": features
        }, ensure_ascii=False, allow_nan=False).encode("utf-8")

class FixMyStreetHistoryHarvester(Harvester):

    def get_configuration(self) -> HarvesterConfiguration:
        return HarvesterConfiguration(
            name="fixmystreet_history_harvester",
            tags=["FixMyStreet", "History"],
            description="Compares FixMyStreet versions to detect updates",
            content_type="application/json",
            source="fixmystreet_collector",
            dependencies=['fixmystreet_collector'],
            dependencies_limit=[1]
        )

    def harvest(self, source_data: Data, **dependencies_data) -> bytes:
        current_data = json.loads(source_data.data.decode("utf-8"))

        previous_version = dependencies_data.get("fixmystreet_collector")
        previous_data = json.loads(previous_version.data.decode("utf-8"))

        current_features = current_data.get("features", [])
        previous_features = previous_data.get("features", [])

        previous_map = {f["properties"]["id"]: f for f in previous_features}

        for feature in current_features:
            props = feature["properties"]
            fid = props["id"]
            prev = previous_map.get(fid)

            if prev:
                prev_props = prev["properties"]
                if props["updatedDate"] != prev_props["updatedDate"]:
                    feature["history"] = {
                        "issueId": fid,
                        "newStatus": props["status"],
                        "newDate": props["updatedDate"],
                        "oldStatus": prev_props["status"],
                        "oldDate": prev_props["updatedDate"]
                    }
                else:
                    feature["history"] = None
            else:
                feature["history"] = None

        return json.dumps({
            "type": "FeatureCollection",
            "features": current_features
        }, ensure_ascii=False).encode("utf-8")


run_components([
    FixMyStreetIncidentsCollector(),
    FixMyStreetHistoryHarvester()
])

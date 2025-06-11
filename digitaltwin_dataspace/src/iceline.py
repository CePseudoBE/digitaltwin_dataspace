import json
import geopandas as gpd
import pandas as pd
import requests

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class IrcelineSOSCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="irceline_sos_collector",
            tags=["Irceline", "AirQuality", "GeoJSON"],
            description="Collecte les données de qualité de l'air depuis l'API SOS d'Irceline",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://geo.irceline.be/sos/api/v1/timeseries/?expanded=true"
        response = requests.get(endpoint)
        response.raise_for_status()

        response_json = response.json()
        response_df = pd.json_normalize(response_json, max_level=4)

        # Extraction des coordonnées géographiques
        coords = pd.DataFrame(
            response_df["station.geometry.coordinates"].tolist(),
            columns=["longitude", "latitude"]
        )
        response_df = pd.concat([response_df, coords], axis=1)

        # Création du GeoDataFrame
        gdf = gpd.GeoDataFrame(
            response_df,
            geometry=gpd.points_from_xy(response_df.longitude, response_df.latitude),
            crs="EPSG:4326"
        )

        # Suppression des colonnes inutiles
        columns_to_drop = [
            "station.geometry.coordinates", "station.geometry.type",
            "station.type", "referenceValues", "extras",
            "parameters.service.id", "parameters.service.label",
            "statusIntervals", "longitude", "latitude"
        ]
        gdf = gdf.drop(columns=columns_to_drop, errors='ignore')

        return gdf.to_json().encode('utf-8')

# Pour l'exécuter avec d'autres collecteurs
run_components([
    IrcelineSOSCollector()
])

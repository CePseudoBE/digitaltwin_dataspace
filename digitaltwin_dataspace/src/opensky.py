import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration
import pandas as pd
import json


class OpenSkyCollector(Collector):

    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="opensky_collector",
            tags=["OpenSky+"],
            description="Collects data from OpenSky APIs",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        import math  # nécessaire pour vérifier NaN dans alt
        api_url = (
            "https://opensky-network.org/api/states/all"
            "?lamin=50.775029&lomin=4.193481&lamax=50.962233&lomax=4.578003"
        )

        response = requests.get(api_url)
        response.raise_for_status()

        data = response.json()
        states = data.get("states", [])

        if not states:
            return json.dumps({
                "type": "FeatureCollection",
                "features": []
            }).encode("utf-8")

        columns = [
            "icao24", "callsign", "origin_country", "time_position", "last_contact",
            "longitude", "latitude", "baro_altitude", "on_ground", "velocity", "heading",
            "vertical_rate", "sensors", "geo_altitude", "squawk", "spi", "position_source"
        ]

        df = pd.DataFrame(states, columns=columns)
        df = df.dropna(subset=["latitude", "longitude"])

        features = []
        for i, row in df.iterrows():
            lon = row["longitude"]
            lat = row["latitude"]
            alt = row["geo_altitude"]

            if alt is None or (isinstance(alt, float) and math.isnan(alt)):
                alt = 0

            props = {}
            for k, v in row.items():
                if k in ["longitude", "latitude", "geo_altitude"]:
                    continue
                if isinstance(v, float) and math.isnan(v):
                    v = None
                props[k] = v

            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat, alt]
                },
                "properties": props,
                "id": row["icao24"] or str(i)
            })

        return json.dumps({
            "type": "FeatureCollection",
            "features": features
        }, ensure_ascii=False, allow_nan=False).encode("utf-8")

run_components([
    OpenSkyCollector()
])

import dotenv
dotenv.load_dotenv()
import requests
import pandas as pd
import json
import math

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration


class SensorCommunityCollector(Collector):

    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="sensor_community_collector",
            tags=["Sensor Community"],
            description="Collects data from Sensor Community APIs",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        api_url = "https://data.sensor.community/airrohr/v1/filter/area=50.8503,4.3517,10"
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        df = pd.json_normalize(data, max_level=4)
        df.columns = [col.replace('.', '_') for col in df.columns]

        columns_to_remove = [
            'location_exact',
            'location_altitude',
            'sensor_pin',
            'sensor_sensor_type_id',
            'location_country',
            'sampling_rate',
        ]
        df.drop(columns=columns_to_remove, inplace=True, errors='ignore')

        # Remplacer tous les NaN/NaT par None
        df = df.where(pd.notnull(df), None)

        features = []
        for i, row in df.iterrows():
            lon = row.get("location_longitude")
            lat = row.get("location_latitude")

            if lon is None or lat is None:
                continue

            geometry = {
                "type": "Point",
                "coordinates": [lon, lat]
            }

            props = {}
            for k, v in row.items():
                if k in ("location_longitude", "location_latitude"):
                    continue

                if not isinstance(k, str):
                    props[str(k)] = v
                    continue

                keys = k.split("_")
                current = props
                for j, key in enumerate(keys):
                    if j == len(keys) - 1:
                        if isinstance(v, float) and math.isnan(v):
                            v = None
                        current[key] = v
                    else:
                        current = current.setdefault(key, {})

            features.append({
                "type": "Feature",
                "geometry": geometry,
                "properties": props,
                "id": row.get("sensor_id") or str(i)
            })

        return json.dumps({
            "type": "FeatureCollection",
            "features": features
        }, ensure_ascii=False, allow_nan=False).encode("utf-8")



run_components([
    SensorCommunityCollector()
])

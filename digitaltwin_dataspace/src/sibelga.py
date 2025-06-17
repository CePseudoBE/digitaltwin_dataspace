import json

import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration


class SibelgaCollector(Collector):

    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="sibelga_collector",
            tags=["Sibelga"],
            description="Collects data from Sibelga APIs",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        api_url = "https://www.sibelga.be/fr/chantiers-data/data"
        response = requests.get(api_url)
        response.raise_for_status()

        data = response.json()
        items = data.get("items", [])

        features = []
        for item in items:
            lat = item.get("latitude")
            lon = item.get("longitude")
            if lat is not None and lon is not None:
                item_copy = {k: v for k, v in item.items() if k not in ["latitude", "longitude"]}
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    },
                    "properties": item_copy
                })

        geojson = {
            "type": "FeatureCollection",
            "features": features
        }

        return json.dumps(geojson).encode("utf-8")


if __name__ == "__main__":
    run_components([
        SibelgaCollector()
    ])

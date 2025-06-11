import os
import requests

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class DeLijnGTFSStaticCollector(Collector):
    def get_schedule(self) -> str:
        return "30m"  # Collecte toutes les 30 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="delijn_gtfs_static_collector",
            tags=["DeLijn", "GTFS", "Static"],
            description="Collecte les données GTFS statiques de De Lijn",
            content_type="application/zip"
        )

    def collect(self) -> bytes:
        url = "https://gtfs.irail.be/de-lijn/de_lijn-gtfs.zip"
        response = requests.get(url)
        response.raise_for_status()
        return response.content

class DeLijnGTFSRealtimeCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="delijn_gtfs_realtime_collector",
            tags=["DeLijn", "GTFS", "Realtime"],
            description="Collecte les données GTFS temps réel de De Lijn",
            content_type="application/octet-stream"
        )

    def collect(self) -> bytes:
        endpoint = "https://api.delijn.be/gtfs/v2/realtime?json=false&delay=true&canceled=true"
        response = requests.get(
            endpoint, headers={"Ocp-Apim-Subscription-Key": os.environ["DE_LIJN_API_KEY"]}
        )
        response.raise_for_status()
        return response.content

run_components([
    DeLijnGTFSStaticCollector(),
    DeLijnGTFSRealtimeCollector()
])
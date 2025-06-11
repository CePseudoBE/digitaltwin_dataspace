import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class SNCBGTFSStaticCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"  # Collecte toutes les 10 secondes
    
    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="sncb_gtfs_static_collector",
            tags=["SNCB", "GTFS"],
            description="Collecte les données GTFS statiques de la SNCB",
            content_type="application/zip"
        )
    
    def collect(self) -> bytes:
        endpoint = "https://sncb-opendata.hafas.de/gtfs/static/c21ac6758dd25af84cca5b707f3cb3de"
        return requests.get(endpoint).content

class SNCBGTFSRealtimeCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"  # Collecte toutes les 10 secondes (ajustez si besoin)
    
    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="sncb_gtfs_realtime_collector",
            tags=["SNCB", "GTFS", "realtime"],
            description="Collecte les données GTFS temps réel de la SNCB",
            content_type="application/octet-stream"
        )
    
    def collect(self) -> bytes:
        endpoint = "https://sncb-opendata.hafas.de/gtfs/realtime/c21ac6758dd25af84cca5b707f3cb3de"
        response = requests.get(endpoint)
        if response.status_code >= 500:
            raise ValueError("SNCB gtfs realtime is down.")
        return response.content

run_components([
    SNCBGTFSStaticCollector(),
    SNCBGTFSRealtimeCollector()
])

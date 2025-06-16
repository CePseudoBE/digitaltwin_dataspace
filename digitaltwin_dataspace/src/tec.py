import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class TECGTFSStaticCollector(Collector):
    def get_schedule(self) -> str:
        return "30m"  # Collecte toutes les 30 minutes (ajuste si besoin)

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="tec_gtfs_static_collector",
            tags=["TEC", "GTFS", "Static"],
            description="Collecte les données GTFS statiques de TEC",
            content_type="application/zip"
        )

    def collect(self) -> bytes:
        endpoint = "https://opendata.tec-wl.be/Current%20GTFS/TEC-GTFS.zip"
        return requests.get(endpoint).content

class TECGTFSRealtimeCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les minutes (ajuste si besoin)

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="tec_gtfs_realtime_collector",
            tags=["TEC", "GTFS", "Realtime"],
            description="Collecte les données GTFS temps réel de TEC",
            content_type="application/octet-stream"
        )

    def collect(self) -> bytes:
        endpoint = "https://gtfsrt.tectime.be/proto/RealTime/trips?key=DDEBFA42173D45C08E710C7E9DDE8BDE"
        return requests.get(endpoint).content

run_components([
    TECGTFSStaticCollector(),
    TECGTFSRealtimeCollector()
])

import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class InfrabelLineSectionCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"  # Collecte toutes les 10 minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="infrabel_line_section_collector",
            tags=["Infrabel", "GeoJSON", "Lignes"],
            description="Collecte les sections de lignes du réseau Infrabel (GeoJSON)",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://opendata.infrabel.be/api/explore/v2.1/catalog/datasets/geosporen/exports/geojson?lang=fr&timezone=Europe%2FBerlin"
        return requests.get(endpoint).content

class InfrabelOperationalPointsCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="infrabel_operational_points_collector",
            tags=["Infrabel", "GeoJSON", "Points opérationnels"],
            description="Collecte les points opérationnels du réseau Infrabel (GeoJSON)",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://opendata.infrabel.be/api/explore/v2.1/catalog/datasets/operationele-punten-van-het-netwerk/exports/geojson?lang=fr&timezone=Europe%2FBerlin"
        return requests.get(endpoint).content

class InfrabelPunctualityCollector(Collector):
    def get_schedule(self) -> str:
        return "1h"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="infrabel_punctuality_collector",
            tags=["Infrabel", "JSON", "Ponctualité"],
            description="Collecte les données de ponctualité D-1 du réseau Infrabel (JSON)",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://opendata.infrabel.be/api/explore/v2.1/catalog/datasets/ruwe-gegevens-van-stiptheid-d-1/exports/json?lang=fr&timezone=Europe%2FBerlin"
        return requests.get(endpoint).content

class InfrabelSegmentsCollector(Collector):
    def get_schedule(self) -> str:
        return "10m"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="infrabel_segments_collector",
            tags=["Infrabel", "GeoJSON", "Segments"],
            description="Collecte les segments station à station du réseau Infrabel (GeoJSON)",
            content_type="application/geo+json"
        )

    def collect(self) -> bytes:
        endpoint = "https://infrabel.opendatasoft.com/api/explore/v2.1/catalog/datasets/station_to_station/exports/geojson?lang=fr&timezone=Europe%2FBerlin"
        return requests.get(endpoint).content

run_components([
    InfrabelLineSectionCollector(),
    InfrabelOperationalPointsCollector(),
    InfrabelPunctualityCollector(),
    InfrabelSegmentsCollector()
])

import requests
import dotenv

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class BrusselsMobilityBikeCountersCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les minutes

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="brussels_mobility_bike_counters_collector",
            tags=["Brussels", "Mobility", "Bike", "Counters"],
            description="Collecte les métadonnées des compteurs vélo de Brussels Mobility",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://data.mobility.brussels/bike/api/counts/?request=devices"
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.content

class BrusselsMobilityBikeCountsCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="brussels_mobility_bike_counts_collector",
            tags=["Brussels", "Mobility", "Bike", "Counts"],
            description="Collecte les comptages vélo en temps réel de Brussels Mobility",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://data.mobility.brussels/bike/api/counts/?request=live"
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.content

class BrusselsMobilityTrafficDevicesCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="brussels_mobility_traffic_devices_collector",
            tags=["Brussels", "Mobility", "Traffic", "Devices"],
            description="Collecte les métadonnées des compteurs trafic de Brussels Mobility",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://data.mobility.brussels/traffic/api/counts/?request=devices"
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.content

class BrusselsMobilityTrafficCountsCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="brussels_mobility_traffic_counts_collector",
            tags=["Brussels", "Mobility", "Traffic", "Counts"],
            description="Collecte les comptages trafic en temps réel de Brussels Mobility",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = "https://data.mobility.brussels/traffic/api/counts/?request=live"
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.content

run_components([
    BrusselsMobilityBikeCountersCollector(),
    BrusselsMobilityBikeCountsCollector(),
    BrusselsMobilityTrafficDevicesCollector(),
    BrusselsMobilityTrafficCountsCollector()
])

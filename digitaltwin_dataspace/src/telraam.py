import os
import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class TelraamTrafficCollector(Collector):
    def get_schedule(self) -> str:
        return "1m"  # Collecte toutes les minutes (à adapter selon besoin)

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="telraam_traffic_collector",
            tags=["Telraam", "Traffic", "API"],
            description="Collecte les données de trafic en temps réel depuis l'API Telraam",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        response = requests.get(
            "https://telraam-api.net/v1/reports/traffic_snapshot_live",
            headers={"X-Api-Key": os.environ["TELRAAM_API_KEY"]},
        )
        return response.content


run_components([
    TelraamTrafficCollector()
])

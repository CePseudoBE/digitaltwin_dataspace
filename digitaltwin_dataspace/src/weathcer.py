import os
import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import Collector, ComponentConfiguration, run_components

class OpenWeatherCollector(Collector):
    def get_schedule(self) -> str:
        return "10s"  # Collecte toutes les 5 minutes (modifiable)

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="openweather_collector",
            tags=["OpenWeather", "Météo", "API"],
            description="Collecte les données météo en temps réel pour Bruxelles via OpenWeather",
            content_type="application/json"
        )

    def collect(self) -> bytes:
        endpoint = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?lat=50.8504500&lon=4.3487800&appid={os.environ['OPENWEATHER_API_KEY']}"
        )
        response = requests.get(endpoint)
        return response.content

run_components([
    OpenWeatherCollector()
])

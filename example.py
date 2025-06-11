import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, Harvester, Handler, ComponentConfiguration, \
    HarvesterConfiguration, Data, servable_endpoint


class GoogleCollector(Collector):

    def get_schedule(self) -> str:
        return "10s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="google_collector",
            tags=["Google"],
            description="Collects data from Google APIs",
            content_type="text/html",
        )

    def collect(self) -> bytes:
        return requests.get("https://www.google.com").content


class GoogleHarvester(Harvester):

    def get_configuration(self) -> HarvesterConfiguration:
        return HarvesterConfiguration(
            name="google_harvester",
            tags=["Google"],
            description="Harvests data from Google APIs",
            content_type="text/html",
            source="google_collector",
        )

    def harvest(self, source_data: Data, **dependencies_data) -> bytes:
        # For demonstration, just return the data as is
        return source_data.data[:10]


class GoogleHandler(Handler):
    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="google_handler",
            tags=["Google"],
            description="Handles requests to Google APIs",
            content_type="text/html",
        )

    @servable_endpoint(path="/hi/{name}", response_model=str)
    def say_hi(self, name: str) -> str:
        return f"Hello, {name} from Google!"


run_components([
    GoogleHarvester(),
    GoogleCollector(),
    GoogleHandler()
])

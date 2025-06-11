import json

import dotenv
import requests

dotenv.load_dotenv()

from digitaltwin_dataspace import run_components, Collector, ComponentConfiguration


class EnergyCollection(Collector):

    def get_schedule(self) -> str:
        return "1s"

    def get_configuration(self) -> ComponentConfiguration:
        return ComponentConfiguration(
            name="energy_collector",
            tags=["Energy"],
            description="Collects data from Energy API",
            content_type="application/json",
        )

    def collect(self) -> bytes:
        response = requests.get("http://api.el.sc.ulb.be/energy")
        response.raise_for_status()
        return json.dumps(response.json()).encode("utf-8")

if __name__ == "__main__":
    run_components([
        EnergyCollection()
    ])
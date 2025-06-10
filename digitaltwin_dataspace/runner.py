import logging
import time
from multiprocessing import Process
from typing import List

import fastapi
import schedule
import uvicorn

from .components.base import Component, ScheduleRunnable, Servable
from .utils import schedule_string_to_function

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def _in_process(func):
    def wrapper(*args, **kwargs):
        try:
            process = Process(target=func, args=args, kwargs=kwargs)
            process.start()
            logger.debug(f"Started process for {func.__name__} with PID {process.pid}")
        except Exception as e:
            logger.exception(f"Failed to start process for {func.__name__}: {e}")

    return wrapper


def run_components(components: List[Component]):
    app = fastapi.FastAPI(
        redoc_url="/docs",
        docs_url=None,
    )

    for component in components:
        configuration = component.get_configuration()
        logger.info(f"Registering component: {configuration.name}")

        if isinstance(component, ScheduleRunnable):
            try:
                job = schedule_string_to_function(component.get_schedule())
                job.do(_in_process(component.run))
                logger.info(f"Scheduled {configuration.name} with {component.get_schedule()}")
            except Exception as e:
                logger.exception(f"Failed to schedule {configuration.name}: {e}")

        if isinstance(component, Servable):
            try:
                endpoints = component.get_endpoints()
                for endpoint, method, path, response_model in endpoints:
                    full_path = f"/{configuration.name.replace('_', '-')}{path}"
                    app.add_api_route(
                        path=full_path,
                        endpoint=endpoint,
                        name=configuration.name,
                        description=configuration.description,
                        methods=[method],
                        tags=configuration.tags,
                        response_model=response_model
                    )
                    logger.info(f"Registered endpoint: {method} {full_path}")
            except Exception as e:
                logger.exception(f"Failed to register endpoints for {configuration.name}: {e}")

    def run_app():
        try:
            logger.info("Starting FastAPI app on http://localhost:8080")
            uvicorn.run(app, host="localhost", port=8080,log_level="critical")
        except Exception as e:
            logger.exception("Failed to start FastAPI app", exc_info=e)

    run_app()  # Lancer dans le même process, routes bien présentes

    logger.info("Scheduler started")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logger.exception("Error during scheduler run", exc_info=e)

import abc
from datetime import timedelta, datetime
from typing import List, Optional, Any

from fastapi import Response

from .base import ScheduleRunnable, Servable, Component, servable_endpoint, ComponentConfiguration
from ..data.retrieve import retrieve_latest_row, retrieve_first_row, retrieve_between_datetime, retrieve_after_datetime, \
    retrieve_latest_rows_before_datetime, retrieve_latest_row_before_datetime
from ..data.sync_db import get_or_create_standard_component_table
from ..data.write import write_result

ZERO_DATE = datetime(1970, 1, 1)


def source_range_to_period_and_limit(
        latest_date: datetime, source_range: str | int
) -> (datetime, datetime, int):
    """
    Convert a source range to a period and limit.

    This function takes the latest date and a source range, which can be either a time period or a limit (count).
    If the source range represents a time period, the function calculates the start and end dates of the period,
    rounded to the previous period based on the given time unit. If the source range represents a limit, it returns
    the latest date and the specified limit.

    :param latest_date: The latest date harvested.
    :param source_range: The source range, which can be expressed as a time period or a limit (count).
        Time period examples: "3d" (3 days), "6h" (6 hours), "30m" (30 minutes), "120s" (120 seconds).
        Limit example: "100" (100 records).
    :return: A tuple containing the calculated start date, end date (for time periods), and limit (for counts).
    """

    if source_range is None:
        return latest_date, None, 1

    if type(source_range) == int or source_range.isdigit():
        return latest_date, None, int(source_range)

    if "d" in source_range:
        days = int(source_range.replace("d", ""))
        # Round latest date to the previous period
        latest_date = latest_date - timedelta(days=latest_date.day % days)
        return latest_date, latest_date + timedelta(days=days), None

    elif "h" in source_range:
        hours = int(source_range.replace("h", ""))
        # Round latest date to the previous period
        latest_date = latest_date - timedelta(hours=latest_date.hour % hours)
        return latest_date, latest_date + timedelta(hours=hours), None

    elif "m" in source_range:
        minutes = int(source_range.replace("m", ""))
        # Round latest date to the previous period
        latest_date = latest_date - timedelta(minutes=latest_date.minute % minutes)
        return latest_date, latest_date + timedelta(minutes=minutes), None

    elif "s" in source_range:
        seconds = int(source_range.replace("s", ""))
        # Round latest date to the previous period
        latest_date = latest_date - timedelta(seconds=latest_date.second % seconds)
        return latest_date, latest_date + timedelta(seconds=seconds), None

    return None


class HarvesterConfiguration(ComponentConfiguration):
    source_range: Optional[Any] = None
    source_range_strict: bool = True
    multiple_results: bool = False

    # Component references
    source: Optional[str] = None
    dependencies: Optional[List[str]] = None
    dependencies_limit: Optional[List[int]] = None


class Harvester(Component, ScheduleRunnable, Servable, abc.ABC):
    def run(self):
        configuration = self.get_configuration()
        table = get_or_create_standard_component_table(configuration.name)
        source_table = get_or_create_standard_component_table(
            configuration.source
        )

        # Get latest date harvested
        latest_row = retrieve_latest_row(table, with_null=True)

        if latest_row is None:
            # In case the harvester has never been run, get the first row from the source table
            row = retrieve_first_row(source_table)
            # Minus one second to make sure we include the first row
            latest_date = (row and (row.date - timedelta(seconds=1))) or ZERO_DATE
        else:
            latest_date = latest_row.date

        # Get source range
        start_date, end_date, limit = source_range_to_period_and_limit(
            latest_date, configuration.source_range
        )

        source_data = retrieve_between_datetime(source_table, start_date, end_date, limit)

        if not source_data:
            return False  # No new data to harvest

        if limit and configuration.source_range_strict and len(source_data) < limit:
            return False  # No new data to harvest, still building the amount of data specified by the limit

        if end_date and not retrieve_after_datetime(table, latest_date, 1):
            return False  # No new data to harvest, still building the same period

        storage_date = end_date or source_data[-1].date

        if limit == 1 and not end_date:
            source_data = source_data[0]

        dependencies = configuration.dependencies or []

        dependencies_data = {}

        if dependencies:
            for dependency, dependency_limit in zip(
                    dependencies, configuration.dependencies_limit
            ):
                dependency_table = get_or_create_standard_component_table(dependency)
                dependency_data = retrieve_latest_rows_before_datetime(
                    dependency_table, storage_date, dependency_limit
                )

                if dependency_limit == 1:
                    if not dependency_data:
                        raise ValueError(f"Dependency {dependency} not found")

                    dependency_data = dependency_data[0]
                dependencies_data[dependency] = dependency_data

        result = self.harvest(source_data, **dependencies_data)

        if configuration.multiple_results:
            for item, source in zip(result, source_data):
                write_result(configuration.name, configuration.content_type, table, item,
                             source.date)
        elif result is not None:
            write_result(
                configuration.name, configuration.content_type, table, result, storage_date
            )
        else:
            write_result(
                configuration.name, configuration.content_type, table, None, storage_date
            )

        return True

    def harvest(self, source_data, **dependencies_data):
        """
        Override this method to implement the harvesting logic.
        """
        raise NotImplementedError("The 'harvest' method must be implemented by subclasses.")

    @servable_endpoint(path="/")
    def retrieve(self, timestamp: datetime = None) -> Response:
        data = retrieve_latest_row_before_datetime(
            get_or_create_standard_component_table(self.get_configuration().name),
            timestamp if timestamp else datetime.now(),
        )

        return Response(content=data.data, media_type=data.content_type)

    def get_schedule(self) -> str:
        return "1s"

    def get_configuration(self) -> HarvesterConfiguration:
        """
        Override this method to return the configuration of the harvester.
        """
        raise NotImplementedError("The 'get_configuration' method must be implemented by subclasses.")

import abc
import inspect
from typing import Optional, List, Any, Literal

from pydantic import BaseModel, Field

__all__ = [
    "ComponentConfiguration",
    "Component",
    "ScheduleRunnable",
    "Servable",
    "servable_endpoint",
]




class ComponentConfiguration(BaseModel):
    name: str = Field(...,
                      description="Name of the component, must be unique. It will be used as a key for the table in the database, and in the object store.")
    description: str = Field(..., description="Description of the component, used for documentation purposes.")
    content_type: str = Field(...,
                              description="Content type of the data, 'application/json' or 'application/octet-stream'.")
    tags: Optional[List[str]] = Field(None,
                                      description="List of tags for the component, used for documentation purposes to group endpoints together.")





class Component(abc.ABC):
    @abc.abstractmethod
    def get_configuration(self) -> ComponentConfiguration:
        """Return the configuration of the component."""
        pass


class ScheduleRunnable(abc.ABC):
    @abc.abstractmethod
    def run(self) -> Any:
        """Run the component with the given keyword arguments."""
        pass

    @abc.abstractmethod
    def get_schedule(self) -> str:
        pass


def servable_endpoint(path: str, method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"] = "GET", response_model: Optional[Any] = None):
    def inner(func):
        """Decorator to mark a method as an endpoint for serving."""
        func.is_endpoint = True
        func.path = path
        func.method = method
        func.response_model = response_model
        return func

    return inner


class Servable(abc.ABC):

    def get_endpoints(self):
        for method in inspect.getmembers(self, predicate=inspect.ismethod):
            if hasattr(method[1], "is_endpoint") and method[1].is_endpoint:
                yield method[1], method[1].method, method[1].path,method[1].response_model

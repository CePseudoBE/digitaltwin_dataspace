import abc
from typing import Type

from .base import Component,Servable


class Handler(Component, Servable, abc.ABC):
    pass



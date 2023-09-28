from typing import Type

from .base import Exporter
from .txt_file import TxtAsIs


def get_list_of_validators() -> list[Type[Exporter]]:
    return Exporter.__subclasses__()
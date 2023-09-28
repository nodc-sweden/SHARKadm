from typing import Type

from .base import Validator
from .year import ValidateYearNrDigits
from .unique import ValidateUniqueSampleId


def get_list_of_validators() -> list[Type[Validator]]:
    return Validator.__subclasses__()
from typing import Type

from .base import Validator
from .year import ValidateYearNrDigits
from .unique import ValidateUniqueSampleId


def get_validator_list() -> list[Type[Validator]]:
    return Validator.__subclasses__()


def get_validators() -> dict[str, Type[Validator]]:
    """Returns a dictionary with validators"""
    validators = {}
    for cls in Validator.__subclasses__():
        validators[cls.__name__] = cls
    return validators


def get_validator_object(validator_name: str, **kwargs) -> Validator:
    """Returns Validator object that matches teh given validator name"""
    all_validators = get_validators()
    val = all_validators[validator_name]
    return val(**kwargs)
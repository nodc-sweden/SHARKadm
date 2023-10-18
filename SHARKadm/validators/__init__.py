from typing import Type
import pathlib

from .base import Validator
from .year import ValidateYearNrDigits
from .unique import ValidateUniqueSampleId
from .columns import ValidateColumnViewColumnsNotInDataset


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


def get_validators_description() -> dict[str, str]:
    """Returns a dictionary with validator name as key and the description as value"""
    result = dict()
    for name, val in get_validators().items():
        result[name] = val.get_validator_description()
    return result


def get_validators_description_text() -> str:
    info = get_validators_description()
    line_length = 100
    lines = list()
    lines.append('=' * line_length)
    lines.append('Available validators are:')
    lines.append('-' * line_length)
    for key in sorted(info):
        lines.append(f'{key.ljust(30)}{info[key]}')
    lines.append('=' * line_length)
    return '\n'.join(lines)


def print_validators_description() -> None:
    """Prints all validators on screen"""
    print(get_validators_description_text())


def write_validators_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all validators on screen"""
    with open(path, 'w') as fid: 
        fid.write(get_validators_description_text())
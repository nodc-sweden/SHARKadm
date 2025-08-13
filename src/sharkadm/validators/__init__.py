import functools
import pathlib
from typing import Type

from sharkadm import utils
from sharkadm.utils.inspect_kwargs import get_kwargs_for_class
from sharkadm.validators.aphia_id import (
    ValidateAphiaIdVsBvolAphiaId,
    ValidateReportedVsAphiaId,
    ValidateReportedVsBvolAphiaId,
)
from sharkadm.validators.base import Validator
from sharkadm.validators.column_combination import (
    AssertCombination,
    AssertMinMaxDepthCombination,
)
from sharkadm.validators.columns import ValidateColumnViewColumnsNotInDataset
from sharkadm.validators.common_values import ValidateCommonValuesByVisit
from sharkadm.validators.date_and_time import MissingTime, ValidateDateAndTime
from sharkadm.validators.depth import ValidateSampleDepth, ValidateSecchiDepth
from sharkadm.validators.mandatory import (
    ValidateValuesInMandatoryNatColumns,
    ValidateValuesInMandatoryRegColumns,
)
from sharkadm.validators.occurrence_id import ValidateOccurrenceId
from sharkadm.validators.position import CheckIfLatLonIsSwitched
from sharkadm.validators.positive import ValidatePositiveValues
from sharkadm.validators.station import (
    ValidateCoordinatesDm,
    ValidateNameInMaster,
    ValidatePositionInOcean,
    ValidatePositionWithinStationRadius,
    ValidateStationIdentity,
    ValidateSynonymsInMaster,
)
from sharkadm.validators.year import ValidateYearNrDigits


@functools.cache
def get_validator_list() -> list[Type[Validator]]:
    return sorted(utils.get_all_class_children_names(Validator))


def get_validators() -> dict[str, Type[Validator]]:
    """Returns a dictionary with validators"""
    return utils.get_all_class_children(Validator)


def get_validator_object(name: str, **kwargs) -> Validator:
    """Returns Validator object that matches the given validator name"""
    all_validators = get_validators()
    val = all_validators[name]
    return val(**kwargs)


def get_validators_description() -> dict[str, str]:
    """Returns a dictionary with validator name as key and the description as value"""
    result = dict()
    for name, val in get_validators().items():
        if name.startswith("_"):
            continue
        result[name] = val.get_validator_description()
    return result


def get_validators_info() -> dict:
    result = dict()
    for name, val in get_validators().items():
        result[name] = dict()
        result[name]["name"] = name
        result[name]["description"] = val.get_validator_description()
        result[name]["kwargs"] = get_kwargs_for_class(val)
    return result


def get_validators_description_text() -> str:
    info = get_validators_description()
    line_length = 100
    lines = list()
    lines.append("=" * line_length)
    lines.append("Available validators:")
    lines.append("-" * line_length)
    for key in sorted(info):
        lines.append(f"{key.ljust(60)}{info[key]}")
    lines.append("=" * line_length)
    return "\n".join(lines)


def print_validators_description() -> None:
    """Prints all validators on screen"""
    print(get_validators_description_text())


def write_validators_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all validators on screen"""
    with open(path, "w") as fid:
        fid.write(get_validators_description_text())

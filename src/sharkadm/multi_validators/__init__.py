import functools
import pathlib
from typing import Type

from sharkadm import utils
from sharkadm.utils.inspect_kwargs import get_kwargs_for_class
from .aphia_id import AphiaIdAfter

from .base import MultiValidator


@functools.cache
def get_multi_validator_list() -> list[str]:
    """Returns a sorted list of name of all available multi_validators"""
    return sorted(utils.get_all_class_children_names(MultiValidator))


def get_multi_validators() -> dict[str, Type[MultiValidator]]:
    """Returns a dictionary with multi_validators"""
    return utils.get_all_class_children(MultiValidator)


def get_multi_validator_object(name: str, **kwargs) -> MultiValidator | None:
    """Returns MultiValidator object that matches the given multi validator names"""
    all_trans = get_multi_validators()
    tran = all_trans.get(name)
    if not tran:
        return
    return tran(**kwargs)


def get_multi_validators_description() -> dict[str, str]:
    """Returns a dictionary with multi validator name as key and the description as
    value"""
    result = dict()
    for name, tran in get_multi_validators().items():
        if name.startswith("_"):
            continue
        result[name] = tran.get_validator_description()
    return result


def get_multi_validators_info() -> dict:
    result = dict()
    for name, tran in get_multi_validators().items():
        result[name] = dict()
        result[name]["name"] = name
        result[name]["description"] = tran.get_validator_description()
        result[name]["kwargs"] = get_kwargs_for_class(tran)
    return result


def get_multi_validators_description_text() -> str:
    info = get_multi_validators_description()
    line_length = 100
    lines = list()
    lines.append("=" * line_length)
    lines.append("Available multi validators:")
    lines.append("-" * line_length)
    for key in sorted(info):
        lines.append(f"{key.ljust(40)}{info[key]}")
        lines.append("")
    lines.append("=" * line_length)
    return "\n".join(lines)


def print_multi_validators_description() -> None:
    """Prints all multi validators on screen"""
    print(get_multi_validators_description_text())


def write_multi_validators_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all multi validators on screen"""
    with open(path, "w") as fid:
        fid.write(get_multi_validators_description_text())

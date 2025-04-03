import functools
import pathlib
from typing import Type

from sharkadm import utils
from sharkadm.utils.inspect_kwargs import get_kwargs_for_class
from .base import MultiTransformer
from .bvol import Bvol
from .calculate import Calculate
from .date_time import DateTime
from .dyntaxa import Dyntaxa
from .general_dv import GeneralDV
from .general_final import GeneralFinal
from .general_initial import GeneralInitial
from .lims import Lims
from .location import Location
from .position import Position
from .static_dv import StaticDV
from .translate import Translate
from .worms import Worms

########################################################################################################################
########################################################################################################################
from .date_time import DateTimePolars


@functools.cache
def get_multi_transformer_list() -> list[str]:
    """Returns a sorted list of name of all available multi_transformers"""
    return sorted(utils.get_all_class_children_names(MultiTransformer))


def get_multi_transformers() -> dict[str, Type[MultiTransformer]]:
    """Returns a dictionary with multi_transformers"""
    return utils.get_all_class_children(MultiTransformer)


def get_multi_transformer_object(name: str, **kwargs) -> MultiTransformer | None:
    """Returns MultiTransformer object that matches the given multi transformer names"""
    all_trans = get_multi_transformers()
    tran = all_trans.get(name)
    if not tran:
        return
    return tran(**kwargs)


def get_multi_transformers_description() -> dict[str, str]:
    """Returns a dictionary with multi transformer name as key and the description as
    value"""
    result = dict()
    for name, tran in get_multi_transformers().items():
        if name.startswith("_"):
            continue
        result[name] = tran.get_transformer_description()
    return result


def get_multi_transformers_info() -> dict:
    result = dict()
    for name, tran in get_multi_transformers().items():
        result[name] = dict()
        result[name]["name"] = name
        result[name]["description"] = tran.get_transformer_description()
        result[name]["kwargs"] = get_kwargs_for_class(tran)
    return result


def get_multi_transformers_description_text() -> str:
    info = get_multi_transformers_description()
    line_length = 100
    lines = list()
    lines.append("=" * line_length)
    lines.append("Available multi transformers:")
    lines.append("-" * line_length)
    for key in sorted(info):
        lines.append(f"{key.ljust(40)}{info[key]}")
        lines.append("")
    lines.append("=" * line_length)
    return "\n".join(lines)


def print_multi_transformers_description() -> None:
    """Prints all multi transformers on screen"""
    print(get_multi_transformers_description_text())


def write_multi_transformers_description_to_file(path: str | pathlib.Path) -> None:
    """Prints all multi transformers on screen"""
    with open(path, "w") as fid:
        fid.write(get_multi_transformers_description_text())

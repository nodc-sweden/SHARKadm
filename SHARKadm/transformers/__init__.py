from typing import Type

from .add_sharkadm_id_columns import AddSharkadmId
from .base import Transformer
from .date_time import AddDateAndTimeToAllLevels
from .date_time import AddDatetime
from .date_time import AddSampleMonth
from .depth import AddSampleMinAndMaxDepth
from .depth import AddSectionStartAndEndDepth
from .depth import ReorderSampleMinAndMaxDepth
from .replace_comma_with_dot import ReplaceCommaWithDot
from .add_sampler_area import AddCalculatedSamplerArea
from .position import AddPositionToAllLevels


def get_transformer_list() -> list[str]:
    """Returns a sorted list of name of all available transformers"""
    return sorted([cls.__name__ for cls in Transformer.__subclasses__()])


def get_transformers() -> dict[str, Type[Transformer]]:
    """Returns a dictionary with transformers"""
    trans = {}
    for cls in Transformer.__subclasses__():
        trans[cls.__name__] = cls
    return trans


def get_transformer_object(trans_name: str, **kwargs) -> Transformer:
    """Returns Transformer object that matches teh given transformer names"""
    all_trans = get_transformers()
    tran = all_trans[trans_name]
    return tran(**kwargs)

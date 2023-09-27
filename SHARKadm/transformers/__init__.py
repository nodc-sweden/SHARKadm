from typing import Type

from .add_sharkadm_id_columns import AddSharkadmIdToColumns
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


def get_list_of_transformers() -> list[Type[Transformer]]:
    return Transformer.__subclasses__()


# def get_default_AddSharkadmIdToColumns() -> AddSharkadmIdToColumns:
#     return AddSharkadmIdToColumns.from_default_config()

from functools import cache
from typing import Type

from sharkadm import utils
from sharkadm.data_filter.base import PolarsDataFilter
from sharkadm.data_filter.data_filter_1nm import PolarsDataFilterInside1nm
from sharkadm.data_filter.data_filter_12nm import PolarsDataFilterInside12nm
from sharkadm.data_filter.data_filter_boolean import (
    PolarsDataFilterFalse,
    PolarsDataFilterTrue,
)
from sharkadm.data_filter.data_filter_coordinates import PolarsDataFilterBoundingBox
from sharkadm.data_filter.data_filter_location import (
    PolarsDataFilterLocation,
)
from sharkadm.data_filter.data_filter_match_in_columns import (
    PolarsDataFilterMatchInColumn,
)
from sharkadm.data_filter.data_filter_month import PolarsDataFilterMonths
from sharkadm.data_filter.data_filter_on_land import PolarsDataFilterOnLand
from sharkadm.data_filter.data_filter_parameter import PolarsDataFilterParameter
from sharkadm.data_filter.data_filter_qflag import PolarsDataFilterQflag
from sharkadm.data_filter.data_filter_value import (
    PolarsDataFilterValueEquals,
    PolarsDataFilterValueLessThan,
    PolarsDataFilterValueMoreThan,
)
from sharkadm.data_filter.data_filter_year import PolarsDataFilterYears


@cache
def get_data_filters() -> dict[str, Type[PolarsDataFilter]]:
    """Returns a dictionary with data_filters"""
    return utils.get_all_class_children(PolarsDataFilter)


def get_data_filter_object(name: str, **kwargs) -> PolarsDataFilter | None:
    """Returns PolarsDataFilter object that matches the given data_filter name"""
    all_filters = get_data_filters()
    filt = all_filters.get(name)
    if not filt:
        return
    args = kwargs.pop("args", [])
    return filt(*args, **kwargs)

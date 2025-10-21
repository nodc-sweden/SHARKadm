import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter
from sharkadm.data_filter.data_filter_12nm import PolarsDataFilterInside12nm
from sharkadm.data_filter.data_filter_approved import PolarsDataFilterApprovedData
from sharkadm.data_filter.data_filter_location import (
    PolarsDataFilterRestrictAreaO,
    PolarsDataFilterRestrictAreaR,
    PolarsDataFilterRestrictAreaRred,
)


class PolarsDataFilterRestrictAreaRorO(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        r = PolarsDataFilterRestrictAreaR().get_filter_mask(data_holder)
        o = PolarsDataFilterRestrictAreaO().get_filter_mask(data_holder)
        return r | o


class PolarsDataFilterRestrictAreaRredorO(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        r = PolarsDataFilterRestrictAreaRred().get_filter_mask(data_holder)
        o = PolarsDataFilterRestrictAreaO().get_filter_mask(data_holder)
        return r | o


class PolarsDataFilterApprovedAndOutside12nm(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series | None:
        approved_bool = PolarsDataFilterApprovedData().get_filter_mask(data_holder)
        inside12nm_bool = PolarsDataFilterInside12nm().get_filter_mask(data_holder)
        outside12nm_bool = ~inside12nm_bool
        return approved_bool | outside12nm_bool


class PolarsDataFilterInside12nmAndNotRestricted(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series | None:
        restrict_boolean = PolarsDataFilterRestrictAreaR().get_filter_mask(data_holder)
        not_restrict_boolean = ~restrict_boolean
        inside12nm_bool = PolarsDataFilterInside12nm().get_filter_mask(data_holder)
        return inside12nm_bool & not_restrict_boolean

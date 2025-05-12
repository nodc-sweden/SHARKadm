from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd
import polars as pl

from sharkadm.data import PandasDataHolder, PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import approved_data


class DataFilter(ABC):
    """Create child class to filter data in data_holder"""

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def get_filter_mask(self, data_holder: PandasDataHolder) -> pd.Series | None: ...


class PolarsDataFilter(ABC):
    """Create child class to filter data in data_holder"""

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._invert: bool = False

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def _get_filter_mask(
        self, data_holder: PolarsDataHolder
    ) -> pl.expr.expr.Expr | None: ...

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def __and__(self, other):
        return PolarsCombinedDataFilter(self, _and=other)

    def __or__(self, other):
        return PolarsCombinedDataFilter(self, _or=other)

    def __invert__(self):
        obj = self.__class__(*self._args, **self._kwargs)
        obj._invert = True
        return obj

    def get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.expr.expr.Expr | None:
        mask = self._get_filter_mask(data_holder)
        if self._invert:
            return ~mask
        return mask


class PolarsCombinedDataFilter:
    def __init__(
        self,
        mask: PolarsDataFilter | PolarsCombinedDataFilter,
        _and: PolarsDataFilter | PolarsCombinedDataFilter = None,
        _or: PolarsDataFilter | PolarsCombinedDataFilter = None,
    ):
        self._mask = mask
        self._and = _and
        self._or = _or
        self._invert = False

    def __repr__(self):
        string = f"{self._mask}"
        if self._and:
            string = " and ".join([string, f"{self._and}"])
        elif self._or:
            string = " or ".join([string, f"{self._or}"])
        return f"({string})"

    def __and__(self, other):
        return PolarsCombinedDataFilter(self, _and=other)

    def __or__(self, other):
        return PolarsCombinedDataFilter(self, _or=other)

    # def __invert__(self):
    #     self._invert = True
    def __invert__(self):
        obj = self.__class__(self._mask, _and=self._and, _or=self._or)
        obj._invert = True
        return obj

    @property
    def name(self) -> str:
        return str(self)

    def get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.expr.expr.Expr | None:
        if self._and:
            mask = self._mask.get_filter_mask(data_holder) & self._and.get_filter_mask(
                data_holder
            )
        elif self._or:
            mask = self._mask.get_filter_mask(data_holder) | self._or.get_filter_mask(
                data_holder
            )
        else:
            mask = self._mask.get_filter_mask(data_holder)

        if self._invert:
            return ~mask
        return mask


class DataFilterRestrictDepth(DataFilter):
    col_to_check = "location_wb"

    def get_filter_mask(self, data_holder: PandasDataHolder) -> pd.Series | None:
        col = "location_wb"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise
        col = "location_county"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise

        # boolean_wb = data_holder.data['location_wb'] != 'N'
        boolean_wb = (data_holder.data["location_wb"] == "Y") | (
            data_holder.data["location_wb"] == "P"
        )
        boolean_county = data_holder.data["location_county"] != ""
        return boolean_wb | boolean_county
        # return data_holder.data['location_wb'] != 'N'


class PolarsDataFilterRestrictAreaR(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        if "location_r" in data_holder.data:
            return data_holder.data["location_r"]
        return (
            data_holder.data["location_ra"]
            | data_holder.data["location_rb"]
            | data_holder.data["location_rc"]
            | data_holder.data["location_rg"]
            | data_holder.data["location_rh"]
            # | data_holder.data["location_ro"]
        )


class PolarsDataFilterLocation(PolarsDataFilter):
    def __init__(self, locations: list[str]):
        super().__init__()
        self._locations = locations

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        mask = data_holder.data[self._locations[0]]
        for col in self._locations[1:]:
            mask = mask | data_holder.data[col]
        return mask


class PolarsDataFilterRestrictAreaO(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        return data_holder.data["location_ro"]


class PolarsDataFilterRestrictAreaRorO(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        r = PolarsDataFilterRestrictAreaR().get_filter_mask(data_holder)
        o = PolarsDataFilterRestrictAreaO().get_filter_mask(data_holder)
        return r | o


class PolarsDataFilterApprovedData(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        if "approved_key" not in data_holder.data:
            adm_logger.log_workflow(
                "Could not create data filter mask. Missing column approved_key",
                level=adm_logger.ERROR,
            )
            return pl.Series()
        ap_data = approved_data.ApprovedData()
        mapper = ap_data.mapper
        return data_holder.data.select(
            pl.col("approved_key").replace_strict(mapper, default=False)
        )["approved_key"]


class PolarsDataFilterYears(PolarsDataFilter):
    def __init__(self, years: list[str | int]):
        super().__init__(years=years)
        self._str_years = [str(y) for y in years]

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        return (
            data_holder.data.with_columns(
                ok_years=pl.col("visit_year").is_in(self._str_years)
            )
            .select("ok_years")
            .to_series()
        )


class PolarsDataFilterInside12nm(PolarsDataFilter):
    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series | None:
        col = "location_wb"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise
        col = "location_county"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise

        # boolean_wb = data_holder.data['location_wb'] != 'N'
        boolean_wb = (data_holder.data["location_wb"] == "Y") | (
            data_holder.data["location_wb"] == "P"
        )
        boolean_county = data_holder.data["location_county"] != ""
        return boolean_wb | boolean_county


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


class working_PolarsDataFilterDeepestDepthRowsForEachVisit(PolarsDataFilter):
    visit_id_columns = (
        "shark_sample_id_md5",
        "visit_date",
        "sample_date",
        "sample_time",
        "sample_latitude_dd",
        "sample_longitude_dd",
        "platform_code",
        "visit_id",
    )

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series | None:
        pass


if __name__ == "__main__":
    approved_filter = PolarsDataFilterApprovedData()
    year_filter = PolarsDataFilterYears(years=list(range(2018, 2025)))
    r_filter = PolarsDataFilterRestrictAreaR()
    o_filter = PolarsDataFilterRestrictAreaO()
    inside_12nm_filter = PolarsDataFilterInside12nm()
    outside_12nm_filter = ~inside_12nm_filter

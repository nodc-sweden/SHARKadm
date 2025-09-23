import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterYears(PolarsDataFilter):
    def __init__(self, *years: str):
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

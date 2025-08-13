import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.data_filter.base import PolarsDataFilter


class PolarsDataFilterMonths(PolarsDataFilter):
    def __init__(self, months: list[str | int]):
        super().__init__(months=months)
        self._str_months = [str(m).zfill(2) for m in months]

    def _get_filter_mask(self, data_holder: PolarsDataHolder) -> pl.Series:
        return (
            data_holder.data.with_columns(
                ok_month=pl.col("visit_month").is_in(self._str_months)
            )
            .select("ok_month")
            .to_series()
        )

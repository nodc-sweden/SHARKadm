import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsAddRowNumber(PolarsTransformer):
    col_to_set = "row_number"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds row number. This column can typically be used to reference data in "
            "log. Transformer should be set by the controller when setting the data "
            "holder"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_set in data_holder.data:
            self._log(f"Column {self.col_to_set} already present. Will not overwrite")
            return
        data_holder.data = data_holder.data.with_row_index(self.col_to_set, 1).cast(
            pl.String
        )

import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.transformers.base import PolarsTransformer

from ..utils import add_column

CALC_BY_DC_MARKER = "Y"

COL_COUNTED_CALCULATED = "COPY_VARIABLE.Calculated # counted.ind"
COL_COUNTED = "COPY_VARIABLE.# counted.ind"
# COL_COUNTED_REPORTED = "count_reported"
COL_COUNTED_REPORTED_FLOAT = "count_reported_float"

COL_COUNTED_CALC_BY_DC = "calc_by_dc_counted"

COL_COEFFICIENT = "coefficient"
COL_COEFFICIENT_FLOAT = "coefficient_float"


class PolarsCalculateSealsNrCounted(PolarsTransformer):
    valid_data_types = ("ringedseal",)
    valid_data_structures = ("column",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Calculating seals # counted."

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        # data_holder.data = data_holder.data.with_columns(
        #     pl.col(COL_COUNTED).alias(COL_COUNTED_REPORTED)
        # )

        self._add_float_columns(data_holder)

        boolean = data_holder.data[COL_COUNTED_REPORTED_FLOAT].is_not_null() & (
            data_holder.data[COL_COEFFICIENT_FLOAT].is_not_null()
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(
                (pl.col(COL_COUNTED_REPORTED_FLOAT) * pl.col(COL_COEFFICIENT_FLOAT))
                .round(4)
                .cast(str)
            )
            .otherwise(pl.lit(""))
            .alias(COL_COUNTED_CALCULATED),
            pl.when(boolean)
            .then(pl.lit(CALC_BY_DC_MARKER))
            .otherwise(pl.lit(""))
            .alias(COL_COUNTED_CALC_BY_DC),
        )

    def _add_float_columns(self, data_holder: PolarsDataHolder):
        data_holder.data = add_column.add_float_column(
            data_holder.data, COL_COUNTED, column_name=COL_COUNTED_REPORTED_FLOAT
        )
        data_holder.data = add_column.add_float_column(
            data_holder.data, COL_COEFFICIENT, column_name=COL_COEFFICIENT_FLOAT
        )


class PolarsFixCalcByDcSeals(PolarsTransformer):
    valid_data_types = ("ringedseal",)
    valid_data_structures = ("row",)
    col_to_set = "calc_by_dc"
    check_columns = (("Calculated # counted", COL_COUNTED_CALC_BY_DC),)

    @staticmethod
    def get_transformer_description() -> str:
        return "Arranging calc_by_dc from calculated variables"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_empty_col_to_set(data_holder)
        for par, source_col in self.check_columns:
            boolean = (pl.col(source_col) == CALC_BY_DC_MARKER) & (
                pl.col("parameter") == par
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(boolean)
                .then(pl.lit(CALC_BY_DC_MARKER))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set),
                # pl.when(boolean)
                # .then(pl.lit(par))
                # .otherwise(pl.col(self.col_to_set))
                # .alias(self.col_to_set),
            )

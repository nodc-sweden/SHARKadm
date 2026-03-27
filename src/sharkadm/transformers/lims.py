import polars as pl

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsMoveLessThanFlagRowFormat(PolarsTransformer):
    valid_data_structures = ("row",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Moves flag < in value column to quality_flag column"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.when(
                (pl.col("value").str.starts_with("<")) & (pl.col("quality_flag") == "")
            )
            .then(pl.lit("<"))
            .otherwise(pl.col("quality_flag"))
            .alias("quality_flag")
        )

        affected_rows = len(data_holder.data.filter(pl.col("value").str.starts_with("<")))

        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col("value").str.starts_with("<"))
            .then(pl.col("value").str.strip_prefix("<"))
            .otherwise(pl.col("value"))
            .alias("value")
        )

        if affected_rows:
            self._log(f'Moving {affected_rows} "<"-flags to quality_flag column')


class PolarsMoveLargerThanFlagRowFormat(PolarsTransformer):
    valid_data_structures = ("row",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Moves flag > in value column to quality_flag column"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.when(
                (pl.col("value").str.starts_with(">")) & (pl.col("quality_flag") == "")
            )
            .then(pl.lit(">"))
            .otherwise(pl.col("quality_flag"))
            .alias("quality_flag")
        )

        affected_rows = len(data_holder.data.filter(pl.col("value").str.starts_with(">")))

        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col("value").str.starts_with(">"))
            .then(pl.col("value").str.strip_prefix(">"))
            .otherwise(pl.col("value"))
            .alias("value")
        )

        if affected_rows:
            self._log(f'Moving {affected_rows} ">"-flags to quality_flag column')


class PolarsRemoveNonDataLines(PolarsTransformer):
    valid_data_holders = ("PolarsLimsDataHolder",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes SLA and ZOO lines in data"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.filter(
            ~pl.col("sample_id").str.contains_any(["-SLA_", "-ZOO_"])
        )


class PolarsKeepOnlyJellyfishLines(PolarsTransformer):
    valid_data_holders = ("PolarsLimsDataHolder",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Keep all rows identified as jellyfish rows from LIMS export"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        samp_nr_col = "SMPNO"
        if samp_nr_col not in data_holder.data.columns:
            samp_nr_col = "sample_id"
        conds = [
            pl.col(samp_nr_col).str.contains("ZOO_")
            & (
                (pl.col("JEL_MNDEP") != "")
                | (pl.col("JEL_MXDEP") != "")
                | (pl.col("JEL_FLOWM_READING") != "")
                | (pl.col("TOT_VOL") != "")
                | (pl.col("FILT_VOL") != "")
                | (pl.col("EXT_VOL") != "")
                | (pl.col("DISP_VOL") != "")
            )
        ]

        data_holder.data = data_holder.data.filter(conds)

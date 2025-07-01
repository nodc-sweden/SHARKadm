import pandas as pd
import polars as pl

from .base import (
    DataHolderProtocol,
    PolarsDataHolderProtocol,
    PolarsTransformer,
    Transformer,
)


class MoveLessThanFlagRowFormat(Transformer):
    valid_data_structures = ("row",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Moves flag < in value column to quality_flag column"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        boolean = data_holder.data["value"].str.startswith("<")

        qf_boolean = data_holder.data["value"] != ""

        move_boolean = boolean & qf_boolean
        data_holder.data.loc[move_boolean, "quality_flag"] = "<"

        data_holder.data["value"] = data_holder.data["value"].str.lstrip("<")

        nr_flags = boolean.value_counts().get(True)
        if nr_flags:
            self._log(f'Moving {nr_flags} "<"-flags to quality_flag column')


class MoveLargerThanFlagRowFormat(Transformer):
    valid_data_structures = ("row",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Moves flag > in value column to quality_flag column"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        boolean = data_holder.data["value"].str.startswith(">")

        qf_boolean = data_holder.data["value"] != ""

        move_boolean = boolean & qf_boolean
        data_holder.data.loc[move_boolean, "quality_flag"] = ">"

        data_holder.data["value"] = data_holder.data["value"].str.lstrip(">")

        nr_flags = boolean.value_counts().get(True)
        if nr_flags:
            self._log(f'Moving {nr_flags} ">"-flags to quality_flag column')


class PolarsMoveLessThanFlagRowFormat(PolarsTransformer):
    valid_data_structures = ("row",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Moves flag < in value column to quality_flag column"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
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

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
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


class MoveLessThanFlagColumnFormat(Transformer):
    valid_data_structures = ("column",)
    valid_data_holders = ("LimsDataHolder",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._q_prefix = "Q_"

    @staticmethod
    def get_transformer_description() -> str:
        return "Moves flag < in value column to Q_-column"

    def _get_q_col(self, col: str) -> str:
        return f"{self._q_prefix}{col}"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.data.columns[:]:
            q_col = self._get_q_col(col)
            if q_col not in data_holder.data.columns:
                continue
            data_holder.data[q_col] = data_holder.data.apply(
                lambda row, c=col: self._add_to_q_column(c, row), axis=1
            )

        for col in data_holder.data.columns[:]:
            q_col = self._get_q_col(col)
            if q_col not in data_holder.data.columns:
                continue
            data_holder.data[col] = data_holder.data[col].apply(lambda x: x.lstrip("<"))

    def _add_to_q_column(self, col: str, row: pd.Series) -> str:
        q_col = self._get_q_col(col)
        if not row[col].startswith("<"):
            return row[q_col]
        if row[q_col]:
            self._log(f"Will not overwrite flag {row[q_col]} with flag <")
            return row[q_col]
        self._log(f"Moving {col} flag < to flag column")
        return "<"


class RemoveNonDataLines(Transformer):
    valid_data_holders = ("LimsDataHolder",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes SLA and ZOO lines in data"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        sla_bool = data_holder.data["sample_id"].str.contains("-SLA_")
        zoo_bool = data_holder.data["sample_id"].str.contains("-ZOO_")
        remove_bool = sla_bool | zoo_bool
        data_holder.data = data_holder.data[~remove_bool]


class PolarsRemoveNonDataLines(PolarsTransformer):
    valid_data_holders = ("PolarsLimsDataHolder",)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes SLA and ZOO lines in data"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
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

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
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

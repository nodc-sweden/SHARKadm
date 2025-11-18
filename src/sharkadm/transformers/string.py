import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from .base import PolarsTransformer
from ..data import PolarsDataHolder


class _PolarsToUppercase(PolarsTransformer):
    apply_on_columns: tuple = ()

    def __init__(self, apply_on_columns: tuple[str] | None = None) -> None:
        super().__init__()
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

    @staticmethod
    def get_transformer_description() -> str:
        return f"Converts all values to uppercase"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for col in self.apply_on_columns:
            if col not in data_holder.data.columns:
                continue
            for (val, ), df in data_holder.data.group_by(col):
                upper_val = val.upper()
                if val == upper_val:
                    continue
                data_holder.data = data_holder.data.with_columns(
                    pl.when(pl.col(col) == val)
                    .then(pl.lit(upper_val))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
                self._log(f"Value in column {col} converted to uppercase: "
                          f"{val} -> {upper_val} ({len(df)} places)",
                          level=adm_logger.INFO)


class PolarsCodesToUppercase(_PolarsToUppercase):
    apply_on_columns = (
        "sampling_laboratory_code",
        "analytical_laboratory_code",
        "sample_orderer_code",
        "species_flag_code",
        "sample_project_code",
    )

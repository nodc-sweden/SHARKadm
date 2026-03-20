import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer


class PolarsDivide(PolarsTransformer):
    valid_data_structures = ("column",)
    key_word = "DIVIDE"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Looks for the {PolarsDivide.key_word} "
            f"key word in all columns and divides accordingly."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        columns = [col for col in data_holder.columns if self.key_word in col]
        for col in columns:
            for (val,), df in data_holder.data.group_by(col):
                data_holder.data = data_holder.data.with_columns(
                    pl.when(pl.col(col) == val)
                    .then(self._calculate(val, col))
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        self._fix_column_names(data_holder=data_holder)

    def _calculate(self, x: str, col: str) -> str:
        if not x:
            return ""
        try:
            denominator = int(col.split(".")[-1])
            fraction = float(x) / denominator
            return str(fraction)
        except Exception as e:
            self._log(
                f"Can not divide. {e}",
                level=adm_logger.WARNING,
            )
            return x

    def _fix_column_names(self, data_holder: PolarsDataHolder):
        name_mapper = dict(
            (col, col.replace(self.key_word, "")) for col in data_holder.columns
        )
        data_holder.data = data_holder.data.rename(name_mapper)


class PolarsMultiply(PolarsTransformer):
    valid_data_structures = ("column",)
    key_word = "Multiply"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Looks for the {PolarsMultiply.key_word} "
            f"key word in all columns and multiply accordingly."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        columns = [col for col in data_holder.columns if self.key_word in col]
        for col in columns:
            for (val,), df in data_holder.data.group_by(col):
                data_holder.data = data_holder.data.with_columns(
                    pl.when(pl.col(col) == val)
                    .then(self._calculate(val, col))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
        self._fix_column_names(data_holder=data_holder)

    def _calculate(self, x: str, col: str) -> str:
        if not x:
            return ""
        try:
            factor = int(col.split(".")[-1])
            product = float(x) * factor
            return str(product)
        except Exception as e:
            self._log(
                f"Can not multiply. {e}",
                level=adm_logger.WARNING,
            )
            return x

    def _fix_column_names(self, data_holder: PolarsDataHolder):
        name_mapper = dict(
            (col, col.replace(self.key_word, "")) for col in data_holder.columns
        )
        data_holder.data = data_holder.data.rename(name_mapper)

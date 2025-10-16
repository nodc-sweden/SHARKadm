import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import DataHolderProtocol, PolarsTransformer, Transformer


class Divide(Transformer):
    key_word = "DIVIDE"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Looks for the {Divide.key_word} "
            f"key word in all columns and divides accordingly."
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.data.columns:
            if self.key_word not in col:
                continue
            data_holder.data[col] = data_holder.data[col].apply(
                lambda x, col=col: self._calculate(x, col)
            )

            # data_holder.data[col] = data_holder.data[col].apply(
            #     lambda row, col=col: self._calculate(col, row), axis=1
            # )
        self._fix_column_names(data_holder=data_holder)

    def _calculate(self, x: str, col: str) -> str:
        if not x:
            self._log("Could not DIVIDE: missing value", item=col)
            return ""
        denominator = int(col.split(".")[-1])
        try:
            if "," in x:
                x = x.replace(",", ".")
                self._log(
                    "Can not divide. Probably a problem with comma in data. "
                    "Consider adding parameter to transformer.",
                    level=adm_logger.INFO,
                    item=col,
                )
            fraction = float(x) / denominator
            return str(fraction)
        except ValueError:
            self._log(
                "Can not divide. Probably a problem with comma in data. Consider adding "
                "parameter to transformer.",
                level=adm_logger.WARNING,
                item=col,
            )
            return x

    def _fix_column_names(self, data_holder: DataHolderProtocol):
        new_column_names = [
            col.replace(self.key_word, "") for col in data_holder.data.columns
        ]
        data_holder.data.columns = new_column_names


class Multiply(Transformer):
    key_word = "MULTIPLY"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Looks for the {Multiply.key_word} "
            f"key word in all columns and multiply accordingly."
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        for col in data_holder.data.columns:
            if self.key_word not in col:
                continue
            data_holder.data[col] = data_holder.data[col].apply(
                lambda x, col=col: self._calculate(x, col)
            )

            # data_holder.data[col] = data_holder.data[col].apply(
            #     lambda row, col=col: self._calculate(col, row),
            #                                                     axis=1)
        self._fix_column_names(data_holder=data_holder)

    def _calculate(self, x: str, col: str) -> str:
        if not x:
            self._log("Could not MULTIPLY: missing value", item=col)
            return ""
        factor = int(col.split(".")[-1])
        product = float(x) * factor
        return str(product)

    def _fix_column_names(self, data_holder: DataHolderProtocol):
        new_column_names = [
            col.replace(self.key_word, "") for col in data_holder.data.columns
        ]
        data_holder.data.columns = new_column_names


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

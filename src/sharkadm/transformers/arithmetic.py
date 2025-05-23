from ..sharkadm_logger import adm_logger
from .base import DataHolderProtocol, Transformer


class Divide(Transformer):
    key_word = "DIVIDE"

    @staticmethod
    def get_transformer_description() -> str:
        return "Looks for the DIVIDE key word in all columns and divides accordingly."

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
        return "Looks for the MULTIPLY key word in all columns and divides accordingly."

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

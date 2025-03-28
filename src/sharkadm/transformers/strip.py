from .base import Transformer, DataHolderProtocol
import polars as pl


class StripAllValues(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Strips all values in data'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.map(lambda x: x.strip() if type(x) == str else x)


class StripAllValuesPolars(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Strips all values in data'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(pl.col(pl.Utf8).str.strip_chars())





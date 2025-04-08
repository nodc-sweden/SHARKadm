import polars as pl

from .base import DataHolderProtocol, Transformer


class ConvertFromPandasToPolars(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Converts data_holder.data from pandas dataframe to polars dataframe"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = pl.from_pandas(data_holder.data)


class ConvertFromPolarsToPandas(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Converts data_holder.data from polars dataframe to pandas dataframe"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.to_pandas()

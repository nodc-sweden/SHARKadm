from .base import Transformer, DataHolderProtocol
from ..utils import matching_strings
import polars as pl
import pandas as pd


class ConvertFromPandasToPolars(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Converts data_holder.data from pandas dataframe to polars dataframe'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = pl.from_pandas(data_holder.data)


class ConvertFromPolarsToPandas(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Converts data_holder.data from polars dataframe to pandas dataframe'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.to_pandas()
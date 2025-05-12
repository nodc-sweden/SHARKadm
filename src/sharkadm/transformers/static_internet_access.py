from .base import Transformer, DataHolderProtocol, PolarsDataHolderProtocol
from sharkadm.data.archive import get_archive_data_holder_names
import polars as pl

class AddStaticInternetAccessInfo(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = 'internet_access'
    text_to_set = 'https://shark.smhi.se'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds links to where you can find the data. This information is static!'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = self.text_to_set


class PolarsAddStaticInternetAccessInfo(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = 'internet_access'
    text_to_set = 'https://shark.smhi.se'

    @staticmethod
    def get_transformer_description() -> str:
        return (f'Adds links to where you can find the data in column {PolarsAddStaticInternetAccessInfo.col_to_set}. '
                f'This information is static!')

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(pl.lit(self.text_to_set).alias(self.col_to_set))

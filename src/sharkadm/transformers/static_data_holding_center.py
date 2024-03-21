from .base import Transformer, DataHolderProtocol
from sharkadm.data import get_archive_data_holder_names


class AddStaticDataHoldingCenter(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = 'data_holding_centre'
    text_to_set = 'Swedish Meteorological and Hydrological Institute (SMHI)'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds name of data holding center. This information is static!'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = self.text_to_set
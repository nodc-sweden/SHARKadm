from .base import Transformer, DataHolderProtocol
from sharkadm.data.archive import get_archive_data_holder_names


class AddStaticDataHoldingCenterEnglish(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = 'data_holding_centre'
    text_to_set = 'Swedish Meteorological and Hydrological Institute (SMHI)'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sets {AddStaticDataHoldingCenterEnglish.col_to_set} to {AddStaticDataHoldingCenterEnglish.text_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = self.text_to_set


class AddStaticDataHoldingCenterSwedish(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = 'data_holding_centre'
    text_to_set = 'Sveriges Meteorologiska och Hydrologiska Institut (SMHI)'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sets {AddStaticDataHoldingCenterSwedish.col_to_set} to {AddStaticDataHoldingCenterSwedish.text_to_set}'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = self.text_to_set
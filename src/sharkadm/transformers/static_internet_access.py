from .base import Transformer, DataHolderProtocol
from sharkadm.data.archive import get_archive_data_holder_names


class AddStaticInternetAccessInfo(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = 'internet_access'
    text_to_set = 'https://shark.smhi.se'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds links to where you can find the data. This information is static!'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = self.text_to_set

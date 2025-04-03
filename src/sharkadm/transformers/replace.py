from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger
from sharkadm.utils import matching_strings
import re


class ReplaceNanWithEmptyString(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return f"Replaces all nan values in data with empty string"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data.fillna("", inplace=True)

from sharkadm.config import adm_config_paths
from sharkadm.data.archive import ArchiveDataHolder
from sharkadm.utils import yaml_data

from .base import Transformer

STATUS_CONFIG = yaml_data.load_yaml(
    adm_config_paths("delivery_note_status"), encoding="utf8"
)


class SetStatusDataHost(Transformer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Sets status columns as checked by data host"

    def _transform(self, data_holder: ArchiveDataHolder) -> None:
        for col, value in STATUS_CONFIG["deliverer_and_datahost"].items():
            data_holder.data[col] = value


class SetStatusDeliverer(Transformer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Sets status columns as checked by deliverer"

    def _transform(self, data_holder: ArchiveDataHolder) -> None:
        for col, value in STATUS_CONFIG["deliverer"].items():
            data_holder.data[col] = value

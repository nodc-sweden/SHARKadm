import polars as pl

from sharkadm.config import adm_config_paths
from sharkadm.utils import yaml_data

from ..data import PolarsDataHolder
from .base import PolarsTransformer

if _config_path := adm_config_paths("delivery_note_status"):
    STATUS_CONFIG = yaml_data.load_yaml(_config_path, encoding="utf8")
else:
    STATUS_CONFIG = {}


class SetStatusDataHost(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Sets status columns as checked by data host"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        args = []
        for col, value in STATUS_CONFIG["deliverer_and_datahost"].items():
            args.append(pl.lit(value).alias(col))
        data_holder.data = data_holder.data.with_columns(args)


class SetStatusDeliverer(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Sets status columns as checked by deliverer"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        args = []
        for col, value in STATUS_CONFIG["deliverer"].items():
            args.append(pl.lit(value).alias(col))
        data_holder.data = data_holder.data.with_columns(args)

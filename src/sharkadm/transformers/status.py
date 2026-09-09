import polars as pl
from nodc_config import nodc_conf

from sharkadm.utils import yaml_data

from ..data import PolarsDataHolder
from .base import PolarsTransformer


def get_status_config() -> dict:
    if _config_path := nodc_conf("delivery_note_status"):
        return yaml_data.load_yaml(_config_path, encoding="utf8")
    return dict()


class SetStatusDataHost(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Sets status columns as checked by data host"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        args = []
        for col, value in get_status_config()["deliverer_and_datahost"].items():
            args.append(pl.lit(value).alias(col))
        data_holder.data = data_holder.data.with_columns(args)


class SetStatusDeliverer(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Sets status columns as checked by deliverer"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        args = []
        for col, value in get_status_config()["deliverer"].items():
            args.append(pl.lit(value).alias(col))
        data_holder.data = data_holder.data.with_columns(args)

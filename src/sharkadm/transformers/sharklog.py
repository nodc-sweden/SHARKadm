import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import PolarsTransformer

nodc_sharklog = None
try:
    import nodc_sharklog
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class AddSharklogEventUuid(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_sharklog:
            self._log(
                "Could not add worms scientific name. "
                "Package nodc-worms not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        if "event_uuid" not in data_holder.data.columns:
            data_holder.data = data_holder.data.with_columns(
                pl.lit("").alias("event_uuid")
            )
        nodc_sharklog.sync_event_uuid(data_holder.config, data_holder.data)

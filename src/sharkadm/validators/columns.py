import logging

import polars as pl

from sharkadm import config
from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import DataHolderProtocol, Validator

logger = logging.getLogger(__name__)


class ValidateColumnViewColumnsNotInDataset(Validator):
    def __init__(self):
        super().__init__()
        self._column_views = config.get_column_views_config()

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Checks which columns in column views that are not present in dataset. "
            "Use this as an early validation"
        )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in self._column_views.get_columns_for_view(data_holder.data_type):
            if col in data_holder.data.columns:
                continue
            adm_logger.log_validation_failed(f"Column view column not in data: {col}")


class ValidateUnmappedColumnsHasData(Validator):
    @staticmethod
    def get_validator_description() -> str:
        return (
            "Checks which columns in column views that are not present in dataset. "
            "Use this as an early validation"
        )

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        for fr, to in data_holder.mapped_columns.items():
            if fr in ["source"]:
                continue
            if fr != to:
                continue
            if not len(data_holder.data.filter(pl.col(fr) != "")):
                continue
            adm_logger.log_validation_failed(f"Unmapped column {fr} has values")

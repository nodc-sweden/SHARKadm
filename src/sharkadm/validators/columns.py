import polars as pl

from sharkadm import config
from sharkadm.sharkadm_logger import adm_logger

from .base import PolarsDataHolder, Validator


class ValidateColumnViewColumnsNotInDataset(Validator):
    @staticmethod
    def get_validator_description() -> str:
        return (
            "Checks which columns in column views that are not present in dataset. "
            "Use this as an early validation"
        )

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        self._column_views = config.get_column_views_config(data_holder.config)
        for col in self._column_views.get_columns_for_view(data_holder.data_type):
            if col in data_holder.data.columns:
                continue
            self._log_fail(f"Column view column not in data: {col}")


class ValidateUnmappedColumnsHasData(Validator):
    @staticmethod
    def get_validator_description() -> str:
        return "Checks which unmapped columns has data. Use this as an early validation"

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        for col in data_holder.unmapped_columns:
            if col not in data_holder.data.columns:
                continue
            if not len(data_holder.data.filter(pl.col(col) != "")):
                continue
            self._log_fail(f"Unmapped column {col} has values")


class ValidateListUnmappedColumns(Validator):
    @staticmethod
    def get_validator_description() -> str:
        return "Logs columns that has not been mapped."

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        self._log_fail(
            f"Unmapped columns: {data_holder.unmapped_columns}", level=adm_logger.WARNING
        )

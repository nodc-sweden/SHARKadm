import logging

from sharkadm import config
from sharkadm.sharkadm_logger import adm_logger

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
            adm_logger.log_validation(f"Column view column not in data: {col}")

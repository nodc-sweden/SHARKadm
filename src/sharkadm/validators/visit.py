import logging

from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Validator

logger = logging.getLogger(__name__)


class ValidateVisitNrSamples(Validator):
    visit_id_column = "custom_visit_id"

    @staticmethod
    def get_validator_description() -> str:
        return "Checks if visits are reasonable in terms of nr samples"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in self._column_views.get_columns_for_view(data_holder.data_type):
            if col in data_holder.data.columns:
                continue
            adm_logger.log_validation_failed(f"Column view column not in data: {col}")

from .base import Validator, DataHolderProtocol

from sharkadm import adm_logger
import logging

logger = logging.getLogger(__name__)


class ValidateOccurrenceId(Validator):
    col_to_check = "unique_id_occurrence"

    @staticmethod
    def get_validator_description() -> str:
        return "Check if occurrence id is present"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_check not in data_holder.data:
            adm_logger.log_validation(
                f"Could not validate occurrence id. Missing column: {self.col_to_check}"
            )
            return
        missing_boolean = data_holder.data[self.col_to_check].str.strip() == ""
        df = data_holder.data.loc[missing_boolean, :]
        if df.empty:
            return
        adm_logger.log_validation(f"Missing {self.col_to_check} ({len(df)} places)")

from .base import Validator, DataHolderProtocol

from sharkadm import adm_logger
import logging

logger = logging.getLogger(__name__)


class CheckIfLatLonIsSwitched(Validator):
    lat_col = "latitude"
    lon_col = "longitude"

    @staticmethod
    def get_validator_description() -> str:
        return "Check if latitude and longitude is reported in wrong order"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if self.unique_column not in data_holder.data.columns:
            adm_logger.log_validation(
                f"Could not check unique sample id. No column named {self.unique_column}",
                level="error",
            )
            return
        duplicates = set(
            data_holder.data[data_holder.data[self.unique_column].duplicated()][
                self.unique_column
            ]
        )
        for dup in duplicates:
            adm_logger.log_validation(f"Duplicate in {self.unique_column} = {dup}")

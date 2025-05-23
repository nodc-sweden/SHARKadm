import logging

from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Validator

logger = logging.getLogger(__name__)


class CheckIfLatLonIsSwitched(Validator):
    lat_col = "latitude"
    lon_col = "longitude"

    @staticmethod
    def get_validator_description() -> str:
        return "Check if latitude and longitude is reported in wrong order"

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        if self.unique_column not in data_holder.data.columns:
            adm_logger.log_validation_failed(
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
            adm_logger.log_validation_failed(f"Duplicate in {self.unique_column} = {dup}")

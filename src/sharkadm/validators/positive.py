import logging

from sharkadm import adm_logger
from .base import Validator, DataHolderProtocol

logger = logging.getLogger(__name__)


class ValidatePositiveValues(Validator):
    cols_to_validate = [
        'air_pressure_hpa',
        'wind_direction_code',
        'weather_observation_code',
        'cloud_observation_code',
        'wave_observation_code',
        'ice_observation_code',
        'wind_speed_ms',
        'water_depth_m',
    ]

    @staticmethod
    def get_validator_description() -> str:
        return f'Checks that all values are positive in columns: {ValidatePositiveValues.cols_to_validate}'

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for col in self.cols_to_validate:
            if col not in data_holder.data:
                continue
            adm_logger.log_validation(f'Checking that all values are positive in column {col}', level=adm_logger.DEBUG)
            for val, df in data_holder.data.groupby(col):
                if not val:
                    continue
                if float(val) < 0:
                    adm_logger.log_validation(f'Negative values found in colum {col} LINES: {sorted(df["row_number"])}' , level=adm_logger.WARNING)




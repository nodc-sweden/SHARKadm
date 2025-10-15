import logging
import re
from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Validator
from ..data import PolarsDataHolder

logger = logging.getLogger(__name__)


class ValidateReportedPosition(Validator):
    # LAT_PATTERN = re.compile(r"^-?(\d+)(\d{2})(?:\.(\d+))?$")
    LAT_PATTERNS = [
        re.compile(r"^\d{2,4}\.?\d*$"),
        re.compile(r"^\d{7}\.?\d*$"),
    ]
    LON_PATTERNS = [
        re.compile(r"^\d{2,4}\.?\d*$"),
        re.compile(r"^\d{6}\.?\d*$"),
    ]

    lat_columns = [
        "visit_reported_latitude",
        "sample_reported_latitude",
        "reported_latitude"
    ]

    lon_columns = [
        "visit_reported_longitude",
        "sample_reported_longitude",
        "reported_longitude"
    ]

    @staticmethod
    def get_validator_description() -> str:
        return "Checks valid formats for reported longitude och latitude columns"

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        lat_columns = [col for col in self.lat_columns if col in data_holder.data.columns]
        lon_columns = [col for col in self.lon_columns if col in data_holder.data.columns]

        for (lat, lon), df in data_holder.data.group_by(lat_columns[0], lon_columns[0]):
            for lat_col in self.lat_columns:
                for reg in self.LAT_PATTERNS:
                    if reg.match(lat):
                        break
                else:
                    self._log_fail(f"Reported latitude {lat} in column {lat_col} has unknown format",
                                   row_numbers=list(df["row_number"]))
            for lon_col in self.lon_columns:
                for reg in self.LON_PATTERNS:
                    if reg.match(lon):
                        break
                else:
                    self._log_fail(f"Reported longitude {lon} in column {lon_col} has unknown format",
                                   row_numbers=list(df["row_number"]))




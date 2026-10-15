import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.validators import Validator
from sharkadm.validators.base import PolarsDataHolder

VISIT_COLUMNS = [
    "sample_latitude_dd",
    "sample_longitude_dd",
    "datetime",
    "reported_station_name",
]


class ValidateSealsHaveCountSum(Validator):
    @staticmethod
    def get_validator_description() -> str:
        return "Check if sum of seals are correct"

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        for (lat, lon, dtime, statn), df in data_holder.data.group_by(VISIT_COLUMNS):
            if "# counted" in df["parameter"]:
                continue
            self._log_fail(
                f"No # counted found for {statn} ({dtime}). Present "
                f"parameters are: {', '.join(df['parameter'])}"
            )


class ValidateSumSeals(Validator):
    @staticmethod
    def get_validator_description() -> str:
        return "Check if sum of seals are correct"

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        for (lat, lon, dtime, statn), df in data_holder.data.group_by(VISIT_COLUMNS):
            if "# counted" not in df["parameter"]:
                adm_logger.log_validation(
                    f"No # counted found for {statn} ({dtime}). Present "
                    f"parameters are: {', '.join(df['parameter'])}",
                    level=adm_logger.DEBUG,
                )
                continue
            sum_seals = sum(
                df.filter(pl.col("parameter") != "# counted")["value"].cast(pl.Int64)
            )
            nr_counted = int(df.filter(pl.col("parameter") == "# counted")["value"][0])
            if sum_seals == nr_counted:
                continue
            self._log_fail(
                f"Incorrect sum of seals for {statn} ({dtime}). Present "
                f"{nr_counted} ({sum_seals})"
            )

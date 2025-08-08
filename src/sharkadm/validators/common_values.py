import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateCommonValuesByVisit(Validator):
    _display_name = "Unique visit data"

    unique_columns = (
        "sample_date",
        "sample_time",
        "sample_latitude_dd",
        "sample_longitude_dd",
        "reported_station_name",
        "water_depth_m",
        "visit_id",
        "expedition_id",
        "platform_code",
        "wind_direction_code",
        "wind_speed_ms",
        "air_temperature_degc",
        "air_pressure_hpa",
        "visit_comment",
    )

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Check if these columns have unique values per visit: "
            f"{ValidateCommonValuesByVisit.unique_columns}"
        )

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        for column_name in self.unique_columns:
            if column_name not in data_holder.data.columns:
                adm_logger.log_validation_failed(
                    f"Could not check uniqueness of '{column_name}'. Column not found.",
                    validator=self.get_display_name(),
                    column=column_name,
                    level=adm_logger.WARNING,
                )
                continue

            for visit_key, unique_values in (
                data_holder.data.group_by("visit_key")
                .agg(pl.col(column_name).unique())
                .iter_rows()
            ):
                if len(unique_values) > 1:
                    adm_logger.log_validation_failed(
                        f"Multiple values for '{column_name}' "
                        f"in visit '{visit_key}': {list(unique_values)}",
                        validator=self.get_display_name(),
                        column=column_name,
                        level=adm_logger.ERROR,
                    )
                elif len(unique_values) == 1:
                    adm_logger.log_validation_succeeded(
                        f"Only one value for '{column_name}' "
                        f"in visit '{visit_key}': {unique_values[0]}",
                        validator=self.get_display_name(),
                        column=column_name,
                        level=adm_logger.INFO,
                    )

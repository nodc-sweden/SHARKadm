import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateCommonValuesByVisit(Validator):
    _display_name = "Unique visit data"

    unique_columns = (
        "visit_year",
        "sample_project_code",
        "sample_orderer_code",
        "visit_date",
        "sample_time",
        "sample_enddate",
        "sample_endtime",
        "platform_code",
        "expedition_id",
        "visit_id",
        "reported_station_name",
        "visit_reported_latitude",
        "visit_reported_longitude",
        "positioning_system_code",
        "water_depth_m",
        "visit_comment",
        "nr_depths",
        "wind_direction_code",
        "wind_speed_ms",
        "air_temperature_degc",
        "air_pressure_hpa",
        "weather_observation_code",
        "cloud_observation_code",
        "wave_observation_code",
        "ice_observation_code",
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

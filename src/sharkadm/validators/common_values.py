import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.validators.base import DataHolderProtocol, Validator


class ValidateCommonValuesByVisit(Validator):
    _display_name = "Unique visit data"

    default_columns_to_validate = (
        "visit_year",
        "sample_project_code",
        "sample_orderer_code",
        "sample_enddate",
        "sample_endtime",
        "expedition_id",
        "visit_id",
        "STATION_ID",
        "SITE_ID",
        "COLONY",
        "sea_region",
        "visit_reported_latitude",
        "visit_reported_longitude",
        "positioning_system_code",
        "water_depth_m",
        "nr_depths",
        "monitoring_station_type_code",
        "monitoring_purpose_code",
        "monitoring_program_code",
        "additional_sampling",
        "finding_circumstances",
        "visit_comment",
        "wind_direction_code",
        "wind_speed_ms",
        "air_temperature_degc",
        "air_pressure_hpa",
        "weather_observation_code",
        "cloud_observation_code",
        "wave_observation_code",
        "ice_observation_code",
    )

    visit_columns = (
        "visit_date",
        "visit_time",
        "sample_date",
        "sample_time",
        "platform_code",
        "reported_station_name",
    )

    def __init__(self, columns_to_validate: tuple[str] | None = None) -> None:
        super().__init__()
        if columns_to_validate is None:
            self.columns_to_validate = self.default_columns_to_validate
            adm_logger.log_workflow(
                "Using default columns_to_validate",
                validator=self.get_display_name(),
                level=adm_logger.DEBUG,
            )
        else:
            self.columns_to_validate = columns_to_validate

    @staticmethod
    def get_validator_description() -> str:
        return "Check if metadata columns have unique values per visit."

    def _validate(self, data_holder: DataHolderProtocol) -> None:
        existing_visit_cols = [
            c for c in self.visit_columns if c in data_holder.data.columns
        ]
        existing_cols, missing_columns = [], []
        for c in self.columns_to_validate:
            if c in data_holder.data.columns:
                existing_cols.append(c)
            else:
                missing_columns.append(c)
        if not existing_cols or not existing_visit_cols:
            adm_logger.log_validation_failed(
                "Not enough metadata columns to check for unique values.",
                validator=self.get_display_name(),
                level=adm_logger.WARNING,
            )
            return
        if missing_columns:
            adm_logger.log_workflow(
                f"Ignoring missing columns: {', '.join(missing_columns)}",
                level=adm_logger.DEBUG,
            )
        df = (
            data_holder.data.select(existing_visit_cols + existing_cols)
            .group_by(existing_visit_cols)
            .agg([pl.col(c).n_unique().alias(c) for c in existing_cols])
        )
        violating_cols = [
            c for c in existing_cols if df.select(pl.col(c).max()).item() > 1
        ]
        if not violating_cols:
            adm_logger.log_validation_succeeded(
                "All columns have one unique value per visit. ",
                validator=self.get_display_name(),
                level=adm_logger.INFO,
            )
            return
        violations = df.select(existing_visit_cols + violating_cols).filter(
            pl.any_horizontal(pl.col(c) > 1 for c in violating_cols)
        )
        for row in violations.iter_rows(named=True):
            visit_filter = pl.all_horizontal(
                pl.col(c) == row[c] for c in existing_visit_cols
            )

            bad_cols = [c for c in violating_cols if row[c] > 1]

            values_df = data_holder.data.filter(visit_filter).select(
                [pl.col(c).unique().alias(c) for c in bad_cols]
            )
            visit_date = row.get("visit_date") or row.get("sample_date")
            visit_time = row.get("visit_time") or row.get("sample_time")
            platform_code = row.get("platform_code")
            station = row.get("reported_station_name")
            for c in bad_cols:
                values = values_df[c].to_list()

                adm_logger.log_validation_failed(
                    f"Multiple values for '{c}' at visit "
                    f"{visit_date}_{visit_time}_{platform_code}_{station}: "
                    f"{values}",
                    validator=self.get_display_name(),
                    column=c,
                    level=adm_logger.ERROR,
                )

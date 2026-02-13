from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators import ValidateCommonValuesByVisit


@pytest.mark.parametrize(
    "given_column, given_value_a, given_value_b, given_should_be_checked",
    (
        ("visit_year", "2025", "2024", True),
        ("sample_project_code", "XXX", "ABH", True),
        ("sample_orderer_code", "OLST", "BDLST", True),
        ("sample_enddate", "20250101", "20250102", True),
        ("sample_endtime", "10:21", "10:22", True),
        ("expedition_id", "01", "02", True),
        ("visit_id", "0001", "0002", True),
        ("visit_reported_latitude", "58.1", "58.2", True),
        ("visit_reported_longitude", "11.5", "11.6", True),
        ("positioning_system_code", "GPS", "DGPS", True),
        ("water_depth_m", "100", "99", True),
        ("visit_comment", "Comment X", "Comment Y", True),
        ("wind_direction_code", "10", "20", True),
        ("wind_speed_ms", "3", "4", True),
        ("air_temperature_degc", "19", "20", True),
        ("air_pressure_hpa", "1020", "1021", True),
        ("sample_depth_m", "10", "11", False),
        ("made_up_column", "what", "ever", False),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_conflicting_values_for_same_visit_are_found(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_column,
    given_value_a,
    given_value_b,
    given_should_be_checked,
):
    columns_to_validate = (
        "visit_year",
        "sample_project_code",
        "sample_orderer_code",
        "sample_enddate",
        "sample_endtime",
        "expedition_id",
        "visit_id",
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
    # Given data for a visit with conflicting values for a given column
    base_row = {column: "value" for column in columns_to_validate}
    given_date = "20250101"
    given_time = "10:20"
    given_platform_code = "ZZ99"
    given_station = "SALTHOLMEN"
    assert given_value_a != given_value_b
    data = pl.DataFrame(
        [
            base_row
            | {
                "visit_date": given_date,
                "sample_time": given_time,
                "platform_code": given_platform_code,
                "reported_station_name": given_station,
                given_column: given_value_a,
            },
            base_row
            | {
                "visit_date": given_date,
                "sample_time": given_time,
                "platform_code": given_platform_code,
                "reported_station_name": given_station,
                given_column: given_value_b,
            },
        ]
    )

    # Given a data holder
    given_data_holder = polars_data_frame_holder_class(data)

    # Given the local configurations lists the data type of the data holder as valid
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateCommonValuesByVisit(columns_to_validate).validate(given_data_holder)

    # Then there should be exactly one or zero failed validation messages for
    # that specific validator and column
    # depending on if the column should be checked or not
    all_logs = adm_logger.data
    specific_validation_logs = [
        log
        for log in all_logs
        if log["log_type"] == adm_logger.VALIDATION
        and log["cls"] == "ValidateCommonValuesByVisit"
    ]

    failed_validations_logs = [
        log for log in specific_validation_logs if not log["validation_success"]
    ]

    failed_logs_for_specific_column = [
        log for log in failed_validations_logs if log["column"] == given_column
    ]

    expected_failed_logs = int(given_should_be_checked)
    assert len(failed_logs_for_specific_column) == expected_failed_logs

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
        ("visit_date", "20250101", "20250102", True),
        ("sample_time", "10:20", "10:21", True),
        ("sample_enddate", "20250101", "20250102", True),
        ("sample_endtime", "10:21", "10:22", True),
        ("platform_code", "ZZ99", "10", True),
        ("expedition_id", "01", "02", True),
        ("visit_id", "0001", "0002", True),
        ("reported_station_name", "SALTHOLMEN", "TJÄRNÖ", True),
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
    # Given data for a visit with conflicting values for a given column
    base_row = {column: "value" for column in ValidateCommonValuesByVisit.unique_columns}
    given_key = "20250101_1020_ZZ99_SALTHOLMEN"
    assert given_value_a != given_value_b
    data = pl.DataFrame(
        [
            base_row | {"visit_key": given_key, given_column: given_value_a},
            base_row | {"visit_key": given_key, given_column: given_value_b},
        ]
    )
    print(data)

    # Given a data holder
    given_data_holder = polars_data_frame_holder_class(data)

    # Given the local configurations lists the data type of the data holder as valid
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateCommonValuesByVisit().validate(given_data_holder)

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

from unittest.mock import patch

import pandas as pd
import pytest

from sharkadm import adm_logger
from sharkadm.data import PandasDataHolder
from sharkadm.validators import ValidateCommonValuesByVisit


class PandasDataFrameHolder(PandasDataHolder):
    def __init__(self, data: pd.DataFrame):
        super().__init__()
        self._data = data

    @property
    def data_type(self) -> str:
        return "data_type"

    @property
    def data_type_internal(self) -> str:
        return "data_type_internal"

    @property
    def dataset_name(self) -> str:
        return "dataset_name"

    def get_data_holder_description(self) -> str:
        return "data_holder_description"


@pytest.mark.parametrize(
    "given_column, given_value_a, given_value_b, given_should_be_checked",
    (
        ("sample_id", "ABC", "123", True),
        ("sample_date", "20250101", "20250102", True),
        ("sample_time", "10:20", "10:21", True),
        ("sample_latitude_dd", "16.1", "16.2", True),
        ("sample_longitude_dd", "58.5", "58.6", True),
        ("station_name", "Station X", "Station Y", True),
        ("water_depth_m", "100", "99", True),
        ("visit_id", "Visit 1", "Visit 2", True),
        ("expedition_id", "Expidition 1", "Expidition 2", True),
        ("platform_code", "PLF1", "PLF2", True),
        ("wind_direction_code", "10", "20", True),
        ("wind_speed_ms", "3", "4", True),
        ("air_temperature_degc", "19", "20", True),
        ("air_pressure_hpa", "1020", "1021", True),
        ("visit_comment", "Comment X", "Comment Y", True),
        ("sample_depth_m", "10", "11", False),
        ("made_up_column", "what", "ever", False),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_conflicting_values_for_same_visit_are_found(
    mocked_data_types, given_column, given_value_a, given_value_b, given_should_be_checked
):
    # Given data for a visit with conflicting values for a given column
    base_row = {column: "value" for column in ValidateCommonValuesByVisit.unique_columns}
    given_key = "001"
    assert given_value_a != given_value_b
    data = pd.DataFrame(
        [
            base_row | {"visit_key": given_key, given_column: given_value_a},
            base_row | {"visit_key": given_key, given_column: given_value_b},
        ]
    )

    # Given a data holder
    given_data_holder = PandasDataFrameHolder(data)

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

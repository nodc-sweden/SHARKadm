from unittest.mock import patch

import pandas as pd
import pytest

from sharkadm import adm_logger
from sharkadm.data import PandasDataHolder
from sharkadm.validators import ValidatePositiveValues


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
    "given_column, given_negative_value, given_row_number",
    (("air_pressure_hpa", -1.23, 3),),
)
@patch("sharkadm.config.get_all_data_types")
def test_negative_values_are_identified(
    mocked_data_types, given_column, given_negative_value, given_row_number
):
    # Given data with a negative value in a given column and row
    assert given_negative_value < 0

    given_values = [
        {
            column: f"{12.34 * (0.8 + ((col_n + row_n / 10) % 0.123)):.3f}"
            for col_n, column in enumerate(
                ValidatePositiveValues.cols_to_validate, start=1
            )
        }
        | {"row_number": row_n}
        for row_n in range(1, given_row_number * 2 + 2)
    ]

    given_values[given_row_number - 1][given_column] = str(given_negative_value)

    given_data = pd.DataFrame(given_values)

    # Given a data holder
    given_data_holder = PandasDataFrameHolder(given_data)

    # Given the local configurations lists the data type of the data holder as valid
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidatePositiveValues().validate(given_data_holder)

    # Then there should be exactly one failed validation messages for
    # that specific validator and column
    all_logs = adm_logger.data
    specific_validation_logs = [
        log
        for log in all_logs
        if log["log_type"] == adm_logger.VALIDATION
        and log["cls"] == "ValidatePositiveValues"
    ]

    failed_validations_logs = [
        log for log in specific_validation_logs if not log["validation_success"]
    ]

    failed_logs_for_specific_column = [
        log for log in failed_validations_logs if log["column"] == given_column
    ]

    assert len(failed_logs_for_specific_column) == 1

    # And the row is correctly identified
    fail_row_numbers = map(
        int, failed_logs_for_specific_column[0]["row_number"].split(", ")
    )
    assert given_row_number in fail_row_numbers

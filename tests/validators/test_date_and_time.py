from unittest.mock import patch

import pandas as pd
import pytest

from sharkadm import adm_logger
from sharkadm.validators.date_and_time import ValidateDateFormat


@pytest.mark.parametrize(
    "given_visit_date_string, expected_visit_date_success, "
    "given_sample_date_string, expected_sample_date_success",
    (
        ("", False, "", False),
        ("2025-06-23", True, "2025-06-23", True),
        ("20250623", False, "2025-06-23", True),
        ("2025-06-23", True, "20250623", False),
        ("2025/06/23", False, "20250623", False),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_date_format(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_visit_date_string,
    expected_visit_date_success,
    given_sample_date_string,
    expected_sample_date_success,
):
    # Given data with given date values
    given_data = pd.DataFrame(
        [
            {
                "visit_date": given_visit_date_string,
                "sample_date": given_sample_date_string,
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateDateFormat().validate(given_data_holder)

    # Then there should be exactly two validation messages
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 2

    # And the validation status for visit date is as expected
    log_message = next(log for log in validator_logs if log["column"] == "visit_date")
    assert log_message["validation_success"] == expected_visit_date_success

    # And the validation status for sample date is as expected
    log_message = next(log for log in validator_logs if log["column"] == "sample_date")
    assert log_message["validation_success"] == expected_sample_date_success

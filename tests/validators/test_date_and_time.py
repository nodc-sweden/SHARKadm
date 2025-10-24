from unittest.mock import patch

import freezegun
import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.date_and_time import ValidateDateAndTime


@pytest.mark.parametrize(
    "given_date, expected_success",
    (
        ("2025-08-06", True),
        ("20250806", False),  # No dashes
        ("25-08-06", False),  # No century
        ("2025/08/06", False),  # Slash as delimiter
        ("08-06-2025", False),  # MDY date
        ("2025-13-06", False),  # Non-existing month
        ("2025-08-32", False),  # Non-existing day
        ("2025-8-06", False),  # Single digit month
        ("2025-08-6", False),  # Single digit day
        ("1799-12-31", False),  # Before valid era
        ("2024-02-29", True),  # Leap day
        ("2025-02-29", False),  # No leap day
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_visit_date(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_date,
    expected_success,
):
    # Given data with given visit date
    given_data = pl.DataFrame(
        [
            {
                "visit_key": "ABC",
                "visit_date": given_date,
                "sample_time": "13:37",
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateDateAndTime().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success


@pytest.mark.parametrize(
    "given_time, expected_success",
    (
        ("13:37", True),
        ("13.37", False),  # Period in time
        ("1:37", False),  # Hour with one digit
        ("13:37:00", False),  # Time with seconds
        ("37:13", False),  # Non-existing hour
        ("13:337", False),  # Non-existing minute
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_sample_time(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_time,
    expected_success,
):
    # Given data with given sample time
    given_data = pl.DataFrame(
        [
            {
                "visit_key": "ABC",
                "visit_date": "2025-08-06",
                "sample_time": given_time,
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateDateAndTime().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success


@pytest.mark.parametrize(
    "given_visit_date, given_sample_time, given_validation_time, expected_success",
    (
        ("1950-01-01", "10:00", "2025-08-07 12:00:00", True),  # Passed date
        ("2050-01-01", "10:00", "2025-08-07 12:00:00", False),  # Future date
        ("2025-08-07", "09:00", "2025-08-07 12:00:00", True),  # Same day, passed hour
        ("2025-08-07", "19:00", "2025-08-07 12:00:00", False),  # Same day, future hour
        ("2025-08-07", "12:00", "2025-08-07 12:00:00", True),  # Now
        ("2025-08-07", "12:00", "2025-08-07 11:59:59", False),  # One second in the future
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_future_datetime_not_allowed(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_date,
    given_sample_time,
    given_validation_time,
    expected_success,
):
    # Given data with given visit date and sample time
    given_data = pl.DataFrame(
        [
            {
                "visit_key": "ABC",
                "visit_date": given_visit_date,
                "sample_time": given_sample_time,
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # Given a specific time for the validation
    with freezegun.freeze_time(given_validation_time):
        # When validating the data
        adm_logger.reset_log()
        ValidateDateAndTime().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success

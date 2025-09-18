from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.serial_number import ValidateSerialNumber


@pytest.mark.parametrize(
    "given_timed_serial_numbers, expected_success",
    (
        ((("2025-09-18T12:00:00.000000", 1),), True),  # Single row
        (
            (("2025-09-18T12:00:00.000000", 1), ("2025-09-18T12:00:00.000000", 1)),
            True,
        ),  # Same time, same serial number
        (
            (("2025-09-18T12:00:00.000000", 1), ("2025-09-18T12:00:00.000000", 2)),
            False,
        ),  # Same time, different serial number
        (
            (("2025-09-18T12:00:00.000000", 1), ("2025-09-18T12:00:00.000001", 2)),
            True,
        ),  # Increasing time
        (
            (("2025-09-18T12:00:00.000000", 1), ("2025-09-18T11:59:59.999999", 2)),
            False,
        ),  # Decreasing time
        (
            (("2025-09-18T12:00:00.000000", 1), ("2025-09-18T12:00:00.000001", 999)),
            True,
        ),  # Skipping serial numbers
        (
            (
                ("2025-09-17T21:00:00.000000", 1),
                ("2025-09-17T22:00:00.000000", 2),
                ("2025-09-17T23:00:00.000000", 3),
                ("2025-09-18T00:00:00.000000", 4),
                ("2025-09-18T01:20:00.000000", 5),
                ("2025-09-18T02:40:00.000000", 5),
            ),
            True,  # Passing midnight
        ),
        (
            (
                ("2025-09-18T11:00:00.000000", 1),
                ("2025-09-18T12:00:00.000000", 2),
                (
                    "2025-09-18T13:00:00.000000",
                    4,
                ),  # Increasing time but unordered serial number
                ("2025-09-18T14:00:00.000000", 3),
                ("2025-09-18T15:00:00.000000", 5),
                ("2025-09-18T16:00:00.000000", 6),
            ),
            False,
        ),
        (
            (
                ("2025-09-18T11:00:00.000000", 1),
                ("2025-09-18T14:00:00.000000", 4),
                ("2025-09-18T13:00:00.000000", 3),
                ("2025-09-18T16:00:00.000000", 6),
                ("2025-09-18T12:00:00.000000", 2),
                ("2025-09-18T15:00:00.000000", 5),
            ),
            True,  # Out of order but logically correct
        ),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_serial_number(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_timed_serial_numbers,
    expected_success,
):
    # Given data
    given_data = pl.DataFrame(
        [
            {
                "row_number": n,
                "datetime": given_datetime,
                "SERNO": given_serial_number,
            }
            for n, (given_datetime, given_serial_number) in enumerate(
                given_timed_serial_numbers, start=1
            )
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSerialNumber().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success

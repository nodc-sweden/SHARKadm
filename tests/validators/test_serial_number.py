from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.serial_number import ValidateSerialNumber


@pytest.mark.parametrize(
    "given_timed_serial_numbers, expected_success",
    (
        ((("2025-09-18T12:00:00.000000", "0001"),), True),  # Single row
        (
            (
                ("2025-09-18T12:00:00.000000", "0001"),
                ("2025-09-18T12:00:00.000000", "0001"),
            ),
            True,
        ),  # Same time, same serial number
        (
            (
                ("2025-09-18T12:00:00.000000", "0001"),
                ("2025-09-18T12:00:00.000000", "0002"),
            ),
            False,
        ),  # Same time, different serial number
        (
            (
                ("2025-09-18T12:00:00.000000", "0001"),
                ("2025-09-18T13:00:00.000000", "0001"),
            ),
            False,
        ),  # Different time, same serial number
        (
            (
                ("2025-09-18T12:00:00.000000", "0001"),
                ("2025-09-18T12:00:00.000001", "0002"),
            ),
            True,
        ),  # Increasing time
        (
            (
                ("2025-09-18T12:00:00.000000", "0001"),
                ("2025-09-18T11:59:59.999999", "0002"),
            ),
            False,
        ),  # Decreasing time
        (
            (
                ("2025-09-18T12:00:00.000000", "0001"),
                ("2025-09-18T12:00:00.000001", "0999"),
            ),
            True,
        ),  # Skipping serial numbers
        (
            (
                ("2025-09-17T21:00:00.000000", "0001"),
                ("2025-09-17T22:00:00.000000", "0002"),
                ("2025-09-17T23:00:00.000000", "0003"),
                ("2025-09-18T00:00:00.000000", "0004"),
                ("2025-09-18T01:20:00.000000", "0005"),
                ("2025-09-18T02:40:00.000000", "0006"),
            ),
            True,  # Passing midnight
        ),
        (
            (
                ("2025-09-23T12:00:00.000000", "0001"),
                ("2025-09-23T13:00:00.000000", "0002"),
                ("2025-09-23T14:00:00.000000", "0003"),
                ("2025-09-23T14:00:00.000000", "0003"),
                ("2025-09-23T15:20:00.000000", "0004"),
                ("2025-09-23T16:40:00.000000", "0005"),
            ),
            True,  # Same time for one repeated serial number
        ),
        (
            (
                ("2025-09-23T12:00:00.000000", "0001"),
                ("2025-09-23T13:00:00.000000", "0002"),
                ("2025-09-23T14:00:00.000000", "0003"),
                ("2025-09-23T15:00:00.000000", "0003"),
                ("2025-09-23T16:20:00.000000", "0004"),
                ("2025-09-23T17:40:00.000000", "0005"),
            ),
            False,  # Different time for one repeated serial number
        ),
        (
            (
                ("2025-09-18T11:00:00.000000", "0001"),
                ("2025-09-18T12:00:00.000000", "0002"),
                ("2025-09-18T13:00:00.000000", "0004"),
                ("2025-09-18T14:00:00.000000", "0003"),
                ("2025-09-18T15:00:00.000000", "0005"),
                ("2025-09-18T16:00:00.000000", "0006"),
            ),
            False,  # Increasing time but unordered serial number
        ),
        (
            (
                ("2025-09-18T11:00:00.000000", "0001"),
                ("2025-09-18T14:00:00.000000", "0004"),
                ("2025-09-18T13:00:00.000000", "0003"),
                ("2025-09-18T16:00:00.000000", "0006"),
                ("2025-09-18T12:00:00.000000", "0002"),
                ("2025-09-18T15:00:00.000000", "0005"),
            ),
            True,  # Out of order but logically correct
        ),
        (
            (("2025-09-18T12:00:00.000000", "1"),),
            False,
        ),  # No padding
        (
            (("2025-09-18T12:00:00.000000", "001"),),
            False,
        ),  # Too few zeros in padding
        (
            (("2025-09-18T12:00:00.000000", "00001"),),
            False,
        ),  # Too many zeros in padding
        (
            (("2025-09-18T12:00:00.000000", "   1"),),
            False,
        ),  # Padding with spaces
        (
            (("2025-09-18T12:00:00.000000", "NOT A NUMBER"),),
            False,
        ),  # Non numerical serial numbers
        (
            (("2025-09-18T12:00:00.000000", ""),),
            False,
        ),  # Empty serial numbers
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
                "visit_id": given_serial_number,
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

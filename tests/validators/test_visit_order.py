import datetime
from unittest.mock import patch

import polars as pl
import pytest

from sharkadm import adm_logger
from sharkadm.validators.visit_order import ValidateVisitOrder


@pytest.mark.parametrize(
    "given_timestamps, expected_success",
    (
        (("2025-08-11 12:00",), True),  # One is always chronological
        (("2025-08-11 12:00", "2025-08-11 12:30"), True),  # Same day, later time
        (("2025-08-11 12:00", "2025-08-11 12:00"), True),  # Same time
        (("2025-08-11 12:00", "2025-08-12 12:00"), True),  # Next day, earlier time
        (("2025-08-11 12:00", "2025-08-11 11:59"), False),  # One minute earlier
        (
            (
                "2025-08-11 12:00",
                "2025-08-11 12:01",
                "2025-08-11 12:02",
                "2025-08-11 12:03",
            ),
            True,
        ),  # More values
        (
            (
                "2025-08-11 12:00",
                "2025-08-11 12:00",
                "2025-08-11 12:01",
                "2025-08-11 12:01",
            ),
            True,
        ),  # More values, some duplicates
        (
            (
                "2025-08-11 12:00",
                "2025-08-11 12:01",
                "2025-08-11 12:00",
                "2025-08-11 12:02",
            ),
            False,
        ),  # More values, one bad
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_order_of_visits(
    mocked_data_types, polars_data_frame_holder_class, given_timestamps, expected_success
):
    # Given a series of timestamps
    assert given_timestamps

    # Given data that uses the timestamps per visit (2 rows per visit)
    given_data = pl.DataFrame(
        [
            {
                "datetime": datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M"),
                "visit_id": n,
                "row_number": n,
            }
            for n, timestamp in enumerate(given_timestamps)
        ]
        * 2
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateVisitOrder().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success

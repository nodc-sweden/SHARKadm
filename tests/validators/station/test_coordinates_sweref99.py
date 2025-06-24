from unittest.mock import patch

import pandas as pd
import pytest

from sharkadm import adm_logger
from sharkadm.validators.station.coordinates_sweref99 import ValidateCoordinatesSweref99


@pytest.mark.parametrize(
    "given_latitude_value, given_longitude_value, expected_success",
    (
        ("", "", False),
        ("X", "Y", False),
        ("5799999.999", "500000", False),  # Below lower latitude bound
        ("5800000", "500000", True),  # On lower latitude bound
        ("7500000", "500000", True),  # On upper latitude bound
        ("7500000.001", "500000", False),  # Above upper latitude bound
        ("6500000", "-150000.001", False),  # Below lower longitude bound
        ("6500000", "-150000", True),  # On lower longitude bound
        ("6500000", "1400000", True),  # On upper longitude bound
        ("6500000", "1400000.001", False),  # Above upper longitude bound
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_coordinates_sweref99(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_latitude_value,
    given_longitude_value,
    expected_success,
):
    # Given data with given Sweref 99 coordinates
    given_data = pd.DataFrame(
        [{"LATIT": given_latitude_value, "LONGI": given_longitude_value, "row_number": 1}]
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateCoordinatesSweref99().validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success

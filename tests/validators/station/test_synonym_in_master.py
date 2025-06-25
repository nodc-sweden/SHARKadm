from unittest.mock import patch

import pandas as pd
import pytest

from sharkadm import adm_logger
from sharkadm.validators.station.synonym_in_master import ValidateSynonymsInMaster


@pytest.mark.parametrize(
    "given_station_name, given_master_synonym_list, expected_success",
    (
        ("a1", {"Station 1": {"b2"}}, False),
        ("a1", {"Station A1": {"a1"}}, True),
        ("sTaTiOn AlIaS", {"Station": {"station alias"}}, True),
        (
            "The station",
            {
                "Station A1": {"a1", "The station"},
                "Another station": {"b2", "The Station"},
            },
            True,
        ),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_station_synonym(
    mocked_data_types,
    pandas_data_frame_holder_class,
    given_station_name,
    given_master_synonym_list,
    expected_success,
):
    # Given data with a given station name
    given_data = pd.DataFrame(
        [{"reported_station_name": given_station_name, "row_number": 1}]
    )

    # Given a valid data holder
    given_data_holder = pandas_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # When validating the data
    adm_logger.reset_log()
    ValidateSynonymsInMaster(given_master_synonym_list).validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success

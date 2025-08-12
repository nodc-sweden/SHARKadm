from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers.add_density import PolarsAddDensity, PolarsAddDensityWide


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depths, "
    "given_salinity_psu, given_temperature, expected_success",
    (
        ("ABC123", 58.2, 10.1, 5.0, -7, 20, False),  # erroneous salinity
        ("DEF456", 57.1, 11.0, 5.7, 0, 5.2, True),  # ok data
        ("GHJ789", 56.5, 12.0, -100, 10.3, 10.1, False),  # erroneous depth
    ),
)
@patch("sharkadm.config.get_all_data_types", return_value=[])
def test_validate_add_density_wide(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depths,
    given_salinity_psu,
    given_temperature,
    expected_success,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "visit_key": given_visit_key,
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depths,
            "Salinity CTD": given_salinity_psu,
            "Temperature CTD": given_temperature,
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # There should be no column with in situ density
    # before application of transformer
    assert "in_situ_density" not in given_data_holder.data.columns, (
        "Density column not added yet"
    )

    # Transforming the data
    PolarsAddDensityWide().transform(given_data_holder)

    # After transformation the in situ density column
    # should exist
    assert "in_situ_density" in given_data_holder.data.columns, (
        "Density column was not added"
    )

    density_value = given_data_holder.data["in_situ_density"][0]

    # The calculated density will either be a float
    # or None if in-data are incorrect
    if expected_success:
        assert density_value is not None, "Expected density, but got None"
    else:
        assert density_value is None, f"Expected None, but got value {density_value}"


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depths, "
    "given_parameter, given_value, expected_success",
    (
        (
            ["ABC123", "ABC123"],
            [58.2, 58.2],
            [10.1, 10.1],
            [5.0, 5.0],
            ["Salinity CTD", "Temperature CTD"],
            [-10.0, -1.7],
            False,
        ),  # erroneous salinity
        (
            ["DEF456", "DEF456"],
            [57.1, 57.1],
            [11.0, 11.0],
            [5.0, 5.0],
            ["Salinity CTD", "Temperature CTD"],
            [35.2, 10.1],
            True,
        ),  # ok data
    ),
)
@patch("sharkadm.config.get_all_data_types", return_value=[])
def test_validate_add_density(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depths,
    given_parameter,
    given_value,
    expected_success,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "visit_key": given_visit_key,
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depths,
            "parameter": given_parameter,
            "value": given_value,
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # There should be no column with in situ density
    # before application of the transformer
    assert "in_situ_density" not in given_data_holder.data.columns, (
        "Density column not added yet"
    )

    # Transforming the data
    PolarsAddDensity().transform(given_data_holder)

    # After transformation the in situ density column
    # should exist
    assert "in_situ_density" in given_data_holder.data.columns, (
        "Density column was not added"
    )

    density_value = given_data_holder.data["in_situ_density"][0]

    # The calculated density will either be a float
    # or None if in-data are incorrect
    if expected_success:
        assert density_value is not None, "Expected density, but got None"
    else:
        assert density_value is None, f"Expected None, but got value {density_value}"

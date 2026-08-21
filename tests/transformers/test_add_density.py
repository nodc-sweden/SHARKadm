from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers.add_gsw_parameters import (
    PolarsAddDensity,
    PolarsAddDensityWide,
)


@pytest.mark.parametrize(
    "given_latitude, given_longitude, given_depth, given_salinity, given_temperature",
    (
        (58.2, 10.1, 5.0, -7, 20),  # erroneous salinity
        (56.5, 12.0, -100, 10.3, 10.1),  # erroneous depth
    ),
)
@patch(
    "sharkadm.transformers.base.PolarsTransformer.is_valid_data_holder", return_value=True
)
def test_validate_add_density_wide_erroneous_data(
    mocked_valid_data_holder,
    polars_data_frame_holder_class,
    given_latitude,
    given_longitude,
    given_depth,
    given_salinity,
    given_temperature,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depth,
            "COPY_VARIABLE.Salinity CTD.o/oo psu": given_salinity,
            "COPY_VARIABLE.Temperature CTD.C": given_temperature,
        }
    )
    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)

    # There should be no column with in situ density
    # before application of transformer
    assert (
        "COPY_VARIABLE.Derived in situ density CTD.kg/m3"
        not in given_data_holder.data.columns
    ), "Density column already exist"

    PolarsAddDensityWide("CTD").transform(given_data_holder)

    # After transformation the in situ density column
    # should still not exist
    assert (
        "COPY_VARIABLE.Derived in situ density CTD.kg/m3"
        not in given_data_holder.data.columns
    ), "Density column was not added"


@pytest.mark.parametrize(
    "given_latitude, given_longitude, given_depth, "
    "given_salinity, given_temperature, expected_success",
    (
        (58.2, 10.1, 5.0, 5, 20, True),  # ok data
        (57.1, 11.0, 5.7, 0, 5.2, True),  # ok data
    ),
)
@patch(
    "sharkadm.transformers.base.PolarsTransformer.is_valid_data_holder", return_value=True
)
def test_validate_add_density_wide(
    mocked_valid_data_holder,
    polars_data_frame_holder_class,
    given_latitude,
    given_longitude,
    given_depth,
    given_salinity,
    given_temperature,
    expected_success,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "sample_latitude_dd": given_latitude,
            "sample_longitude_dd": given_longitude,
            "sample_depth_m": given_depth,
            "COPY_VARIABLE.Salinity CTD.o/oo psu": given_salinity,
            "COPY_VARIABLE.Temperature CTD.C": given_temperature,
        }
    )
    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)

    # There should be no column with in situ density
    # before application of transformer
    assert (
        "COPY_VARIABLE.Derived in situ density CTD.kg/m3"
        not in given_data_holder.data.columns
    ), "Density column already exist"

    PolarsAddDensityWide("CTD").transform(given_data_holder)

    # After transformation the in situ density column
    # should exist
    assert (
        "COPY_VARIABLE.Derived in situ density CTD.kg/m3"
        in given_data_holder.data.columns
    ), "Density column was not added"

    density_value = given_data_holder.data[
        "COPY_VARIABLE.Derived in situ density CTD.kg/m3"
    ][0]

    # The calculated density will be a float
    if expected_success:
        assert density_value is not None, "Expected density, but got None"
    else:
        assert density_value is None, f"Expected None, but got value {density_value}"


@pytest.mark.parametrize(
    "given_visit_key, given_latitude, given_longitude, given_depths, "
    "given_parameter, given_value",
    (
        (
            ["ABC123", "ABC123"],
            [58.2, 58.2],
            [10.1, 10.1],
            [5.0, 5.0],
            ["Salinity CTD", "Temperature CTD"],
            [-10.0, -1.7],
        ),  # erroneous salinity
        (
            ["DEF456", "DEF456"],
            [57.1, 57.1],
            [11.0, 11.0],
            [-5.0, -5.0],
            ["Salinity CTD", "Temperature CTD"],
            [35.2, 10.1],
        ),  # erroneous depth
    ),
)
@patch(
    "sharkadm.transformers.base.PolarsTransformer.is_valid_data_holder", return_value=True
)
def test_validate_add_density_erroneous_data(
    mocked_valid_data_holder,
    polars_data_frame_holder_class,
    given_visit_key,
    given_latitude,
    given_longitude,
    given_depths,
    given_parameter,
    given_value,
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

    # There should be no column with in situ density
    # before application of the transformer
    assert "Derived in situ density CTD" not in given_data_holder.data.columns, (
        "Density column already exist"
    )

    # Transforming the data
    PolarsAddDensity("CTD").transform(given_data_holder)

    # After transformation the in situ density column
    # should still not exist
    assert "Derived in situ density CTD" not in given_data_holder.data.columns, (
        "Density column was not added"
    )


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
            [10.0, -1.7],
            True,
        ),  # ok data
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
@patch(
    "sharkadm.transformers.base.PolarsTransformer.is_valid_data_holder", return_value=True
)
def test_validate_add_density(
    mocked_valid_data_holder,
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

    # There should be no column with in situ density
    # before application of the transformer
    assert "Derived in situ density CTD" not in given_data_holder.data.columns, (
        "Density column already exist"
    )

    # Transforming the data
    PolarsAddDensity("CTD").transform(given_data_holder)

    # After transformation the in situ density column
    # should exist
    assert "Derived in situ density CTD" in given_data_holder.data.columns, (
        "Density column was not added"
    )

    density_value = given_data_holder.data["Derived in situ density CTD"][0]

    # The calculated density will be a float
    if expected_success:
        assert density_value is not None, "Expected density, but got None"
    else:
        assert density_value is None, f"Expected None, but got value {density_value}"

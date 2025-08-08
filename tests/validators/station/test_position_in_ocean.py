from unittest.mock import patch

import geopandas as gp
import polars as pl
import pytest
from shapely import Polygon

from sharkadm import adm_logger
from sharkadm.validators.station.position_in_ocean import ValidatePositionInOcean


@pytest.mark.parametrize(
    "given_longitude_value, given_latitude_value, given_ocean_polygon, expected_success",
    (
        (
            "735000",
            "6500000",
            None,  # No ocean shapefile data
            False,
        ),
        (
            "735000",  # In the center
            "6500000",  # In the center
            Polygon(
                (
                    (650000, 6900000),
                    (820000, 6900000),
                    (820000, 6100000),
                    (650000, 6100000),
                )
            ),
            True,
        ),
        (
            "650000.001",  # Just inside western border
            "6100000.001",  # Just inside southern border
            Polygon(
                (
                    (650000, 6900000),
                    (820000, 6900000),
                    (820000, 6100000),
                    (650000, 6100000),
                )
            ),
            True,
        ),
        (
            "650000",  # On the western border
            "6500000",  # In the center
            Polygon(
                (
                    (650000, 6900000),
                    (820000, 6900000),
                    (820000, 6100000),
                    (650000, 6100000),
                )
            ),
            False,
        ),
        (
            "735000",  # In the center
            "6100000",  # On the southern border
            Polygon(
                (
                    (650000, 6900000),
                    (820000, 6900000),
                    (820000, 6100000),
                    (650000, 6100000),
                )
            ),
            False,
        ),
        (
            "819999.999",  # Just inside eastern border
            "6899999.999",  # Just inside northern border
            Polygon(
                (
                    (650000, 6900000),
                    (820000, 6900000),
                    (820000, 6100000),
                    (650000, 6100000),
                )
            ),
            True,
        ),
        (
            "735000",  # In the center
            "6900000.001",  # On the northern border
            Polygon(
                (
                    (650000, 6900000),
                    (820000, 6900000),
                    (820000, 6100000),
                    (650000, 6100000),
                )
            ),
            False,
        ),
        (
            "820000",  # On the eastern border
            "6900000.001",  # In the center
            Polygon(
                (
                    (650000, 6900000),
                    (820000, 6900000),
                    (820000, 6100000),
                    (650000, 6100000),
                )
            ),
            False,
        ),
    ),
)
@patch("sharkadm.config.get_all_data_types")
def test_validate_position_in_ocean(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_longitude_value,
    given_latitude_value,
    given_ocean_polygon,
    expected_success,
):
    # Given data with given coordinates
    given_data = pl.DataFrame(
        [
            {
                "reported_station_name": "Station 1",
                "LATIT": given_latitude_value,
                "LONGI": given_longitude_value,
                "row_number": 1,
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    # Given a shapefile with oceans
    geodata = [{"geometry": given_ocean_polygon}] if given_ocean_polygon else None
    given_shapefile = gp.GeoDataFrame(geodata)

    # When validating the data
    adm_logger.reset_log()
    ValidatePositionInOcean(given_shapefile).validate(given_data_holder)

    # Then there should be exactly one validation message
    all_logs = adm_logger.data
    validator_logs = [log for log in all_logs if log["log_type"] == adm_logger.VALIDATION]
    assert len(validator_logs) == 1

    # And the validation status is as expected
    log_message = validator_logs[0]
    assert log_message["validation_success"] == expected_success

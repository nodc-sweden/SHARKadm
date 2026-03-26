from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers.add_uncertainty import PolarsAddStandardUncertainty


@pytest.mark.parametrize(
    "given_uncert_val, given_metcu, expected_value",
    (
        (1.6, "U2", 0.8),  # expanded uncertainty
        (
            0.75,
            "SD",
            0.75,
        ),  # standard deviation
        (
            1.2,
            "",
            None,
        ),  # no metcu
        (
            1.2,
            "expanded uncertainty",
            None,
        ),  # free text
    ),
)
@patch("sharkadm.config.get_valid_data_types", return_value=[])
def test_validate_polars_add_standard_uncertainty(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_uncert_val,
    given_metcu,
    expected_value,
):
    # Arrange
    given_data = pl.DataFrame(
        {
            "UNCERT_VAL": [given_uncert_val],
            "method_calculation_uncertainty": [given_metcu],
        }
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (
        given_data_holder.data_type_internal,
        given_data_holder.data_type,
    )

    # There should be no column "UNCERT_VAL" in the dataframe
    # before application of transformer
    assert "STD_UNCERT" not in given_data_holder.data.columns, (
        "The column STD_UNCERT already exist."
    )

    # Transforming the data
    PolarsAddStandardUncertainty().transform(given_data_holder)

    # There should be a column "UNCERT_VAL" in the dataframe
    # after application of transformer
    assert "STD_UNCERT" in given_data_holder.data.columns, (
        "The column STD_UNCERT was not added to the dataframe"
    )

    std_uncert = given_data_holder.data.select("STD_UNCERT").to_series()[0]

    # The value in the column UNCERT_VAL should match the expected value
    if std_uncert is not None:
        assert round(std_uncert, 4) == round(expected_value, 4), (
            "The added value to the column STD_UNCERT differs from the expected value"
        )
    else:
        assert expected_value is None, (
            "The added value to the column STD_UNCERT differs from the expected value"
        )

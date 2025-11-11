from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers import AddCtdKust


@pytest.mark.parametrize(
    "given_ctd, given_kust, expected_replacements",
    [
        (
            {
                "COPY_VARIABLE.Temperature CTD.C": "",
                "COPY_VARIABLE.Salinity CTD.o/oo psu": "",
                "COPY_VARIABLE.Conductivity CTD.mS/m": "",
                "COPY_VARIABLE.Dissolved oxygen O2 CTD.ml/l": "",
            },
            {
                "TEMP_CTD_KUST": "20.0",
                "SALT_CTD_KUST": "35.0",
                "CNDC_CTD_KUST": "0.75",
                "DOXY_CTD_KUST": "8.5",
            },
            4,  # All CTD columns should be replaced
        ),
        (
            {
                "COPY_VARIABLE.Temperature CTD.C": "2",
                "COPY_VARIABLE.Salinity CTD.o/oo psu": "31.2",
                "COPY_VARIABLE.Conductivity CTD.mS/m": "540",
                "COPY_VARIABLE.Dissolved oxygen O2 CTD.ml/l": "7.4",
            },
            {
                "TEMP_CTD_KUST": "2.5",
                "SALT_CTD_KUST": "31.3",
                "CNDC_CTD_KUST": "550",
                "DOXY_CTD_KUST": "7.5",
            },
            0,  # No replacement
        ),
    ],
)
@patch("sharkadm.config.get_all_data_types")  # Mocking dependencies for data types
def test_add_ctd_kust(
    mocked_data_types,
    polars_data_frame_holder_class,
    given_ctd,
    given_kust,
    expected_replacements,
):
    # given data
    given_data = pl.DataFrame([{**given_ctd, **given_kust}])

    # given valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    col_mapper = {
        "TEMP_CTD_KUST": "COPY_VARIABLE.Temperature CTD.C",
        "SALT_CTD_KUST": "COPY_VARIABLE.Salinity CTD.o/oo psu",
        "CNDC_CTD_KUST": "COPY_VARIABLE.Conductivity CTD.mS/m",
        "DOXY_CTD_KUST": "COPY_VARIABLE.Dissolved oxygen O2 CTD.ml/l",
    }

    for kust_col, ctd_col in col_mapper.items():
        assert (
            given_data_holder.data[kust_col][0] != given_data_holder.data[ctd_col][0]
        ), f"{kust_col} is identical to {ctd_col} before transformation."

    # Transform the data
    AddCtdKust().transform(given_data_holder)

    # After transformation the number of ctd columns that are identical
    # to the kust columns should match the expected number of replacements

    replacements = sum(
        1
        for kust_col, ctd_col in col_mapper.items()
        if given_data_holder.data[kust_col][0] == given_data_holder.data[ctd_col][0]
    )

    assert replacements == expected_replacements, (
        f"Expected {expected_replacements} replacements, "
        f"but found {replacements} replacements."
    )

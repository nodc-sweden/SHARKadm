from unittest.mock import patch

import polars as pl
import pytest

from sharkadm.transformers.columns import AddColumnsWithPrefix


@pytest.mark.parametrize(
    "apply_on_columns, col_prefix, expected_prefix, expected_number_of_added_columns",
    [
        (
            (
                "visit_year",
                "visit_date",
                "sample_time",
                "expedition_id",
                "visit_id",
            ),
            "reported",
            "reported",
            5,
        ),
        (
            (
                "parameter",
                "value",
                "unit",
            ),
            "original",
            "original",
            3,
        ),
        (
            tuple(),
            "test",
            "test",
            0,
        ),
        (
            (
                "visit_year",
                "sample_time",
                "non_existing_col",
            ),
            "prefix",
            "prefix",
            2,
        ),
    ],
)
@patch("sharkadm.config.get_all_data_types")
def test_add_columns_with_prefix(
    mocked_data_types,
    polars_data_frame_holder_class,
    apply_on_columns,
    col_prefix,
    expected_prefix,
    expected_number_of_added_columns,
):
    # Given data
    given_data = pl.DataFrame(
        [
            {
                "visit_year": "2020",
                "visit_date": "2020-01-01",
                "sample_time": "10:10",
                "expedition_id": "01",
                "visit_id": "001",
                "parameter": "PHOS",
                "value": "0.12",
                "unit": "umol/l",
            }
        ]
    )

    # Given a valid data holder
    given_data_holder = polars_data_frame_holder_class(given_data)
    mocked_data_types.side_effect = (given_data_holder.data_type_internal,)

    assert not any(col_prefix in col for col in given_data_holder.data.columns), (
        "Columns with prefix in their name already exist before transformation."
    )

    # transforming the data
    AddColumnsWithPrefix(apply_on_columns, col_prefix).transform(given_data_holder)

    # After transformation the number of columns that startswith prefix should match
    # the expected number of added columns

    prefix_columns = [
        col for col in given_data_holder.data.columns if col.startswith(col_prefix)
    ]

    assert len(prefix_columns) == int(expected_number_of_added_columns), (
        f"Expected {expected_number_of_added_columns} added columns "
        f"but found {len(prefix_columns)}: {prefix_columns}"
    )

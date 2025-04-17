from sharkadm.transformers import PolarsRemoveColumns
from tests.transformers.utilities import DataframeDataHolder


def test_add_remove_columns(given_data_in_row_format):
    # Given a selection of columns to remove
    given_columns_to_remove = {"value", "datetime"}

    # Given data holder
    given_data_holder = DataframeDataHolder(given_data_in_row_format, {})
    given_transformer = PolarsRemoveColumns(*given_columns_to_remove)

    # Given the data has these columns
    assert given_columns_to_remove <= set(given_data_holder.columns)

    # When transforming the data
    given_transformer.transform(given_data_holder)

    # Then the selected columns are removed
    assert not given_columns_to_remove & set(given_data_holder.columns)

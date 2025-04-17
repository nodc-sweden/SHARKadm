from sharkadm.transformers import PolarsAddAnalyseInfo
from tests.transformers.utilities import DataframeDataHolder


def test_add_analyse_info(given_data_in_row_format, given_analyse_info):
    given_data_holder = DataframeDataHolder(given_data_in_row_format, given_analyse_info)
    given_transformer = PolarsAddAnalyseInfo()

    columns_before = set(given_data_holder.columns)

    # When transforming the data
    given_transformer.transform(given_data_holder)

    # Then the columns of the data after is a true superset of the columns before
    # I.e. there are columns added and no columns are removed
    columns_after = set(given_data_holder.columns)
    assert columns_after > columns_before

from unittest import mock

from sharkadm.data import get_polars_data_holder
from sharkadm.data.lims import PolarsLimsDataHolder


def test_get_polars_data_holder_can_identify_lims_folder(lims_folder):
    # Given a lims data folder
    # When calling get_polars_data_holder
    with mock.patch.object(PolarsLimsDataHolder, "_load_data"):
        data_holder = get_polars_data_holder(lims_folder)

    # Then the data holder is PolarsLimsDataHolder
    assert isinstance(data_holder, PolarsLimsDataHolder)

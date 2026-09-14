from sharkadm.data import is_lims_directory

# def test_get_polars_data_holder_can_identify_lims_folder(lims_folder):
#     # Given a lims data folder
#     # When calling get_polars_data_holder
#     config = Mock(Config)
#     with mock.patch.object(PolarsLimsDataHolder, "_load_data"):
#         data_holder = get_polars_data_holder(config, lims_folder)
#
#     # Then the data holder is PolarsLimsDataHolder
#     assert isinstance(data_holder, PolarsLimsDataHolder)


def test_get_polars_data_holder_can_identify_lims_folder(lims_folder):
    is_lims = is_lims_directory(lims_folder)
    assert bool(is_lims) is True

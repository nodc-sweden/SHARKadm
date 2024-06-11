from . import data
from sharkadm.data import get_data_holder


def test_get_data_holder():
    holder = get_data_holder(data.CHLOROPHYLL_COLUMN_DATA_PATH.parent.parent)

    assert holder.data_type == 'Chlorophyll'
    assert holder.data_format == 'Chlorophyll'
    assert holder.dataset_name == 'SHARK_Chlorophyll_2021_SMHI_version_2023-04-27'
    assert holder.data_holder_name == 'ChlorophyllArchiveDataHolder'

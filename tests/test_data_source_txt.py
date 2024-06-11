from . import data
from sharkadm.data.data_source import txt_file


def test_data_source_txt_row():
    # Load data
    txt_obj = txt_file.TxtRowFormatDataFile(data.LIMS_DATA_PATH)

    # Fix data
    df = data.get_lims_dataframe()
    cols = [col.strip() for col in df]
    df.columns = cols

    obj_df = txt_obj.data
    obj_df.pop('source')

    # Tests
    assert obj_df.to_dict() == df.to_dict()


def test_data_source_txt_column():
    # Load data
    txt_obj = txt_file.TxtColumnFormatDataFile(data.CHLOROPHYLL_COLUMN_DATA_PATH)

    # Fix data
    df = data.get_phytoplankton_dataframe()
    cols = [col.strip() for col in df]
    df.columns = cols

    obj_df = txt_obj.data
    obj_df.pop('source')

    # Tests
    assert obj_df.to_dict() == df.to_dict()

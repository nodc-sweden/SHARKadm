from sharkadm.config import data_type


def test_get_mapped_datatype_is_mapping_correctly():
    assert data_type._get_mapped_datatype("Plankton Barcoding") == "plankton_barcoding"
    assert data_type._get_mapped_datatype("Physical and Chemical") == "physicalchemical"
    assert data_type._get_mapped_datatype("PhysicalChemical") == "physicalchemical"


def test_get_correct_data_type_object():
    assert isinstance(
        data_type.data_type_handler.get_data_type_obj("unknown"),
        data_type.DataTypeUnknown,
    )
    assert isinstance(
        data_type.data_type_handler.get_data_type_obj("Physical and chemical"),
        data_type.DataTypePhysicalChemical,
    )
    assert isinstance(
        data_type.data_type_handler.get_data_type_obj("phytoplankton"), data_type.DataType
    )

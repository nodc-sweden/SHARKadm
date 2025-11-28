from sharkadm.config import data_type


def test_get_mapped_datatype_is_mapping_correctly():
    assert data_type._get_mapped_datatype("Plankton Barcoding") == "plankton_barcoding"
    assert data_type._get_mapped_datatype("Physical and Chemical") == "physicalchemical"
    assert data_type._get_mapped_datatype("PhysicalChemical") == "physicalchemical"

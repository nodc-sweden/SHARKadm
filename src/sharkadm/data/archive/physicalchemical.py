from .archive_data_holder import PolarsArchiveDataHolder


class PolarsPhysicalChemicalArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "physicalchemical"
    _data_format = "PhysicalChemical"

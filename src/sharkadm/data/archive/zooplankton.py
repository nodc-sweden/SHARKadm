from .archive_data_holder import PolarsArchiveDataHolder


class PolarsZooplanktonArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "zooplankton"
    _data_format = "Zooplankton"

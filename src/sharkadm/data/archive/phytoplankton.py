from .archive_data_holder import PolarsArchiveDataHolder


class PolarsPhytoplanktonArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "phytoplankton"
    _data_format = "Phytoplankton"

from .archive_data_holder import PolarsArchiveDataHolder


class PolarsChlorophyllArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "chlorophyll"
    _data_format = "Chlorophyll"

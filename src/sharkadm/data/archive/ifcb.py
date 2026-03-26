from .archive_data_holder import PolarsArchiveDataHolder


class IfcbArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "ifcb"
    _data_format = "IFCBB"

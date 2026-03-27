from .archive_data_holder import PolarsArchiveDataHolder


class PolarsPrimaryProductionArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "primaryproduction"
    _data_format = "Primaryproduction"

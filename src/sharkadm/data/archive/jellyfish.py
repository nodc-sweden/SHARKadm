from .archive_data_holder import PolarsArchiveDataHolder


class PolarsJellyfishArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "jellyfish"
    _data_format = "Jellyfish"

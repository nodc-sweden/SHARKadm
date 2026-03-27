from .archive_data_holder import PolarsArchiveDataHolder


class PolarsMarineBiotoxinsArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "marinebiotoxins"
    _data_format = "MarineBiotoxins"

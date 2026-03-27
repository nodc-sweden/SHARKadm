from .archive_data_holder import PolarsArchiveDataHolder


class PolarsGreySealArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "greyseal"
    _data_format = "Greyseal"

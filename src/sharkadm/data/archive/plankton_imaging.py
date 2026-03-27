from .archive_data_holder import PolarsArchiveDataHolder


class PlanktonImagingArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "plankton_imaging"
    _data_format = "PlanktonImaging"

import logging

from .archive_data_holder import PolarsArchiveDataHolder

logger = logging.getLogger(__name__)


class PolarsMarineBiotoxinsArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "marinebiotoxins"
    _data_format = "MarineBiotoxins"

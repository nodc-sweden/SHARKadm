import logging

from .archive_data_holder import PolarsArchiveDataHolder

logger = logging.getLogger(__name__)


class PolarsPrimaryProductionArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "primaryproduction"
    _data_format = "Primaryproduction"

import logging

from .archive_data_holder import PolarsArchiveDataHolder

logger = logging.getLogger(__name__)


class PolarsGreySealArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_synonym = "greyseal"
    _data_format = "Greyseal"

import logging

from ...config.data_type import data_type_handler
from .archive_data_holder import PolarsArchiveDataHolder

logger = logging.getLogger(__name__)


class PolarsGreySealArchiveDataHolder(PolarsArchiveDataHolder):
    _data_type_obj = data_type_handler.get_data_type_obj("greyseal")
    _data_format = "Greyseal"

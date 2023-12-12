import datetime

import pandas as pd

from .base import Transformer, DataHolderProtocol
from SHARKadm import adm_logger
from SHARKadm.config import get_column_views_config


class Add(Transformer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_transformer_description() -> str:
        return ''

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        pass



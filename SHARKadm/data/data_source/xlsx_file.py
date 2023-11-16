import logging

import pandas as pd

from .base import DataFile

logger = logging.getLogger(__name__)


class XlsxFormatDataFile(DataFile):

    def __init__(self, *args, **kwargs):
        self._sheet_name = kwargs.pop('sheet_name')
        super().__init__(*args, **kwargs)

    def _load_file(self) -> None:
        self._data = pd.read_excel(self._path, sheet_name=self._sheet_name, dtype=str)



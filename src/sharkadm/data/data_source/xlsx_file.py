import logging

import pandas as pd

from .base import DataFile

logger = logging.getLogger(__name__)


class XlsxFormatDataFile(DataFile):

    def __init__(self, *args, **kwargs):
        self._sheet_name = kwargs.pop('sheet_name')
        self._skip_rows = kwargs.pop('skip_rows')
        super().__init__(*args, **kwargs)

    def _load_file(self) -> None:
        print(f'{self._skip_rows=}')
        self._data = pd.read_excel(self._path, sheet_name=self._sheet_name, dtype=str, skiprows=self._skip_rows)



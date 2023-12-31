import logging

import pandas as pd

from .base import DataFile

logger = logging.getLogger(__name__)


class TxtRowFormatDataFile(DataFile):

    def _load_file(self) -> None:
        self._data = pd.read_csv(self._path, encoding=self._encoding, sep='\t', dtype=str)


class TxtColumnFormatDataFile(DataFile):

    def _load_file(self) -> None:
        self._data = pd.read_csv(self._path, encoding=self._encoding, sep='\t', dtype=str)


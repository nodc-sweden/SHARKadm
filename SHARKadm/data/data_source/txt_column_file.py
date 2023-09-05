import pathlib
import pandas as pd

from .common import DataFile
import logging


logger = logging.getLogger(__name__)


class TxtColumnDataFile(DataFile):

    def _load_file(self) -> None:
        self._data = pd.read_csv(self._path, encoding=self._encoding, sep='\t', dtype=str)
        self._original_header = list(self._data.columns)


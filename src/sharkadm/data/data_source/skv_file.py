import logging

import pandas as pd

from .base import DataFile

logger = logging.getLogger(__name__)


class SkvDataFile(DataFile):
    """This is a csv style file typically linked to other skv files"""

    def _load_file(self) -> None:
        self._data = pd.read_csv(self._path, encoding=self._encoding, sep=";", dtype=str)

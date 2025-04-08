import logging

import pandas as pd
import polars as pl

from .base import DataFile, DataFilePolars

logger = logging.getLogger(__name__)


class CsvRowFormatDataFilePolars(DataFilePolars):
    def __init__(self, *args, delimiter: str = "\t", **kwargs):
        self._delimiter = delimiter
        super().__init__(*args, **kwargs)

    def _load_file(self) -> None:
        self._data = pl.read_csv(
            self._path,
            encoding=self._encoding,
            separator=self._delimiter,
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        )


class TxtRowFormatDataFile(DataFile):
    def _load_file(self) -> None:
        self._data = pd.read_csv(self._path, encoding=self._encoding, sep="\t", dtype=str)


class TxtColumnFormatDataFile(DataFile):
    def _load_file(self) -> None:
        self._data = pd.read_csv(self._path, encoding=self._encoding, sep="\t", dtype=str)

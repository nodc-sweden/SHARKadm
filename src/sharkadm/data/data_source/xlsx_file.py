import logging
from collections import defaultdict

import pandas as pd
import polars as pl

from .base import DataFile, DataFilePolars

logger = logging.getLogger(__name__)


class XlsxFormatDataFile(DataFile):
    def __init__(self, *args, **kwargs):
        self._sheet_name = kwargs.pop('sheet_name')
        self._skip_rows = kwargs.pop('skip_rows')
        super().__init__(*args, **kwargs)

    def _load_file(self) -> None:
        self._data = pd.read_excel(
            self._path,
            sheet_name=self._sheet_name,
            dtype=str,
            skiprows=self._skip_rows,
            has_header=True,
        )

class XlsxFormatDataFilePolars(DataFilePolars):
    def __init__(self, *args, sheet_name: str, skip_rows: int = 0, **kwargs):
        self._sheet_name = sheet_name
        self._skip_rows = skip_rows
        super().__init__(*args, **kwargs)

    def _load_file(self) -> None:
        self._data = pl.read_excel(
            self._path,
            engine="calamine",
            sheet_name=self._sheet_name,
            read_options={"header_row": self._skip_rows, "dtypes": "string"},
            has_header=True,
        )

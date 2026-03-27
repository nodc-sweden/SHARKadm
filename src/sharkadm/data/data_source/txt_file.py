import polars as pl

from .base import PolarsDataFile


class CsvRowFormatPolarsDataFile(PolarsDataFile):
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

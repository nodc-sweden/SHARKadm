import polars as pl

from .base import PolarsDataFile


class SkvDataFile(PolarsDataFile):
    """This is a csv style file typically linked to other skv files"""

    def _load_file(self) -> None:
        self._data = pl.read_csv(
            self._path,
            encoding=self._encoding,
            separator=";",
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        )

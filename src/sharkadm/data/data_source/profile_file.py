import logging

import polars as pl

from .base import PolarsDataFile

logger = logging.getLogger(__name__)


class StandardFormatPolarsDataFile(PolarsDataFile):
    def __init__(self, *args, **kwargs):
        self._separator = kwargs.get(
            "separator", kwargs.get("delimiter", kwargs.get("sep", "\t"))
        )
        self._n_rows = kwargs.get(
            "nr_rows",
            kwargs.get("n_rows", kwargs.get("read_nr_rows", kwargs.get("read_n_rows"))),
        )

        super().__init__(*args, **kwargs)

    def _load_file(self) -> None:
        self._data = pl.read_csv(
            self._path,
            comment_prefix="//",
            encoding=self._encoding,
            separator=self._separator,
            n_rows=self._n_rows,
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        )
        self._add_date_and_time()

    def _add_date_and_time(self):
        self._data = self._data.with_columns(
            pl.concat_str(
                [
                    pl.col("YEAR"),
                    pl.col("MONTH").str.zfill(2),
                    pl.col("DAY").str.zfill(2),
                ],
                separator="-",
            ).alias("SDATE"),
            pl.concat_str(
                [
                    pl.col("HOUR").str.zfill(2),
                    pl.col("MINUTE").str.zfill(2),
                    # pl.col("SECOND").str.zfill(2),
                ],
                separator=":",
            ).alias("STIME"),
        )


class OdvProfilePolarsDataFile(PolarsDataFile):
    def __init__(self, *args, **kwargs):
        self._separator = kwargs.get(
            "separator", kwargs.get("delimiter", kwargs.get("sep", "\t"))
        )
        self._n_rows = kwargs.get(
            "nr_rows",
            kwargs.get("n_rows", kwargs.get("read_nr_rows", kwargs.get("read_n_rows"))),
        )

        super().__init__(*args, **kwargs)

    def _load_file(self) -> None:
        self._data = pl.read_csv(
            self._path,
            comment_prefix="//",
            encoding=self._encoding,
            separator=self._separator,
            n_rows=self._n_rows,
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        )

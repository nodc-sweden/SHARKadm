import pathlib

import polars as pl


class TranslateHeaders:
    def __init__(self, path: str | pathlib.Path, encoding: str = "cp1252"):
        self._path = pathlib.Path(path)
        self._encoding = encoding
        self._sep = "\t"
        self._config: pl.DataFrame = pl.DataFrame()
        self._load_config()

    def _load_config(self) -> None:
        self._config = pl.read_csv(
            self._path, encoding=self._encoding, separator=self._sep
        )
        self._config = pl.read_csv(
            self._path,
            encoding=self._encoding,
            separator=self._sep,
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        )

    def get_mapper(self, to: str, map_from: str = "internal_key") -> dict[str, str]:
        pass

import pathlib
import functools

import polars as pl

NAME_AND_SIZE_SEPARATOR = ":"


class TrophicTypeSMHI:
    def __init__(self, path: str | pathlib.Path, encoding: str = "cp1252"):
        self._path = pathlib.Path(path)
        self._encoding = encoding
        self._sep = "\t"
        self._data: pl.DataFrame = pl.DataFrame()
        self._load_data()

    def _load_data(self) -> None:
        self._data = pl.read_csv(
            self._path,
            encoding=self._encoding,
            separator=self._sep,
            infer_schema=False,
            missing_utf8_is_empty_string=True,
        )
        self._data = self._data.with_columns(pl.concat_str(
            [pl.col("scientific_name", "size_class")],
            separator=NAME_AND_SIZE_SEPARATOR).alias("name_and_size"))

    @functools.cache
    def get_mapper(self) -> dict[str, str]:
        return dict(zip(self._data["name_and_size"], self._data["trophic_type"]))


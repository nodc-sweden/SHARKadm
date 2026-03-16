import pathlib
from typing import Protocol

import polars as pl

from sharkadm.sharkadm_logger import adm_logger


class Mapper(Protocol):
    def mapper(self, external_par: str) -> str: ...


class Sensorinfo:
    def __init__(
        self,
        data: pl.DataFrame,
        mapper: Mapper | None = None,
        path: pathlib.Path | None = None,
    ) -> None:
        self._data = data
        self._mapper = mapper
        self._path = path

        if self._mapper:
            self._map_data()

    def _map_data(self):
        if not self._mapper:
            adm_logger.log_workflow(
                f"No mapper found when trying to map Sensorinfo file: {self._path}"
            )
            return
        self._data = self._data.rename(self._mapper.mapper, strict=False)

    def __str__(self):
        return f"Sensorinfo file: {self._path}"

    def get_info(self, **kwargs) -> list[dict]:
        boolean = pl.Series([True] * len(self._data))
        for key, value in kwargs.items():
            boolean = boolean & (pl.col(key) == value)
        records = self._data.filter(boolean).to_dicts()
        return records

    @classmethod
    def from_sensorinfo_file(
        cls, path: str | pathlib.Path, mapper: Mapper = None, encoding: str = "cp1252"
    ) -> "Sensorinfo":
        data = pl.read_csv(
            path,
            encoding=encoding,
            separator="\t",
            infer_schema=False,
            missing_utf8_is_empty_string=True,
            truncate_ragged_lines=True,
        )
        return Sensorinfo(data, mapper=mapper, path=path)

    @property
    def data(self) -> pl.DataFrame:
        return self._data

    @property
    def parameters(self) -> list[str]:
        """Returns a list of all parameters in the file. The list is unsorted."""
        return list(self._data.columns)

    @property
    def columns(self) -> list[str]:
        return list(next(iter(self.data[next(iter(self._data))])))

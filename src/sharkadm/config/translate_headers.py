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

    @property
    def columns(self) -> list[str]:
        return self._config.columns

    def get_mapper(self, to: str, map_from: str = "internal_key") -> dict[str, str]:
        mapper = dict(zip(self._config[map_from], self._config[to]))
        mapper.update(self._get_no_unit_mapper(mapper))
        return mapper

    def get_to_internal_mapper(self):
        mapper = dict()
        for col in [c for c in self._config.columns if c != "internal_key"]:
            col_mapper = dict(zip(self._config[col], self._config["internal_key"]))
            mapper.update(col_mapper)
        mapper.update(self._get_no_unit_mapper(mapper))
        return mapper

    @staticmethod
    def _get_no_unit_mapper(mapper: dict[str, str]) -> dict[str, str]:
        return dict((key.split("(")[0].strip(), value) for key, value in mapper.items())

import pathlib

import polars as pl

from sharkadm.data.archive import metadata
from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.data.profile import sensorinfo


class PolarsProfileDataHolder(PolarsDataHolder):
    _data_type_synonym = "physicalchemical"
    _data_structure = "profile"
    _dataset_name = "profile"

    _header_mapper = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _sensor_info: dict[str, sensorinfo.Sensorinfo] = dict()

    @property
    def date_column(self) -> str:
        date_col = "SDATE"
        if self._header_mapper:
            date_col = self._header_mapper.get_internal_name(date_col)
        return date_col

    @property
    def time_column(self) -> str:
        time_col = "STIME"
        if self._header_mapper:
            time_col = self._header_mapper.get_internal_name(time_col)
        return time_col

    @property
    def metadata_by_key(self) -> dict:
        meta = {}
        for (source,), df in self.data.group_by("source"):
            key = pathlib.Path(source).stem
            meta[key] = {}
            for col in df.columns:
                values = set(df[col])
                if len(values) != 1:
                    continue
                meta[key][col] = values.pop()
        return meta

    @property
    def metadata_original_columns(self) -> dict:
        if not self._header_mapper:
            return self.metadata_by_key
        meta = {}
        for key, data in self.metadata_by_key.items():
            meta[key] = {}
            for col, value in data.items():
                meta[key][self._header_mapper.get_external_name(col)] = value
        return meta

    @property
    def sensor_info(self) -> dict[str, sensorinfo.Sensorinfo]:
        return self._sensor_info

    @staticmethod
    def get_data_holder_description() -> str:
        return """"""

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)

    def add_metadata(self, data: "metadata.Metadata") -> None:
        for (date, time), df in self._data.group_by(self.date_column, self.time_column):
            boolean = (pl.col(self.date_column) == date) & (
                pl.col(self.time_column) == time
            )
            kw = {self.date_column: date, self.time_column: time[:5]}
            meta = data.get_info(**kw)
            if len(meta) != 1:
                raise Exception("Metadata error")
            exps = []
            for col, value in meta[0].items():
                if col not in self._data.columns:
                    self._data = self._data.with_columns(pl.lit("").alias(col))
                exps.append(
                    pl.when(boolean).then(pl.lit(value)).otherwise(pl.col(col)).alias(col)
                )
            self._data = self._data.with_columns(exps)

    def set_sensor_info(self, info: dict[str, sensorinfo.Sensorinfo]):
        for key, df in info.items():
            if not isinstance(df, sensorinfo.Sensorinfo):
                raise ValueError(f"Not a polars dataframe: {type(df)}")
        self._sensor_info = info

    def get_unit_mapper(self) -> dict[str, str]:
        if not self.data_sources:
            return dict()
        return self.data_sources[next(iter(self.data_sources.keys()))].unit_mapper

import pandas as pd
import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import geography

from ..data import PolarsDataHolder
from .base import (
    DataHolderProtocol,
    PolarsDataHolderProtocol,
    PolarsTransformer,
    Transformer,
)


class AddSamplePositionDD(Transformer):
    lat_col_to_set = "sample_latitude_dd"
    lon_col_to_set = "sample_longitude_dd"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.lat_info = {
            "nr_sweref_digits": 7,
            "columns": (
                "sample_reported_latitude",
                "visit_reported_latitude",
            ),  # In order of prioritization
        }

        self.lon_info = {
            "nr_sweref_digits": 6,
            "columns": (
                "sample_reported_longitude",
                "visit_reported_longitude",
            ),  # In order of prioritization
        }

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sample position based on reported position"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[[self.lat_col_to_set, self.lon_col_to_set]] = (
            data_holder.data.apply(
                lambda row: self._get(row), axis=1, result_type="expand"
            )
        )

    def _get(self, row: pd.Series) -> (str, str):
        lat_col = ""
        lon_col = ""
        for lat_col in self.lat_info["columns"]:
            if lat_col in row and row[lat_col].strip():
                break
        for lon_col in self.lon_info["columns"]:
            if lon_col in row and row[lon_col].strip():
                break

        if not all([lat_col, lon_col]):
            return "", ""

        lat_value = row[lat_col].strip().replace(" ", "")  # .replace(',', '.')
        lon_value = row[lon_col].strip().replace(" ", "")  # .replace(',', '.')

        if not all([lat_value, lon_value]):
            return "", ""

        if self._is_sweref99tm(
            value=lat_value, info=self.lat_info
        ) and self._is_sweref99tm(value=lon_value, info=self.lon_info):
            return geography.sweref99tm_to_decdeg(lon_value, lat_value)
        elif self._is_dd(lat_value) and self._is_dd(lon_value):
            return lat_value, lon_value
        elif self._is_dm_lat(lat_value) and self._is_dm_lon(lon_value):
            return geography.decmin_to_decdeg(lat_value), geography.decmin_to_decdeg(
                lon_value
            )

    def _is_sweref99tm(self, value: str, info: dict) -> bool:
        if len(value.split(".")[0]) == info["nr_sweref_digits"]:
            return True
        return False

    def _is_dm_lat(self, value: str) -> bool:
        parts = value.split(".")
        if len(parts[0].zfill(4)) == 4:
            return True
        return False

    def _is_dm_lon(self, value: str) -> bool:
        parts = value.split(".")
        if len(parts[0].zfill(5)) == 5:
            return True
        return False

    def _is_dd(self, value: str) -> bool:
        parts = value.split(".")
        if len(parts[0].zfill(2)) == 2:
            return True
        return False


class AddSamplePositionSweref99tm(Transformer):
    lat_source_col = "sample_latitude_dd"
    lon_source_col = "sample_longitude_dd"
    y_column_to_set = "sample_sweref99tm_y"
    x_column_to_set = "sample_sweref99tm_x"

    def __init__(self, *args, use_db: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self._use_db = use_db

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sample position in sweref99tm"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.x_column_to_set] = ""
        data_holder.data[self.y_column_to_set] = ""
        for (lat, lon), df in data_holder.data.groupby(
            [self.lat_source_col, self.lon_source_col]
        ):
            if not all([lat, lon]):
                self._log(
                    f"Missing position when trying to set {self.x_column_to_set} "
                    f"and {self.y_column_to_set}"
                )
                continue
            x, y = geography.decdeg_to_sweref99tm(lat=lat, lon=lon, use_db=self._use_db)
            data_holder.data.loc[df.index, self.x_column_to_set] = x
            data_holder.data.loc[df.index, self.y_column_to_set] = y


class PolarsAddSamplePositionSweref99tm(PolarsTransformer):
    lat_source_col = "sample_latitude_dd"
    lon_source_col = "sample_longitude_dd"
    y_column_to_set = "sample_sweref99tm_y"
    x_column_to_set = "sample_sweref99tm_x"

    def __init__(self, *args, use_db: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self._use_db = use_db

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sample position in sweref99tm"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        if self.x_column_to_set in data_holder.data:
            return
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.x_column_to_set),
            pl.lit("").alias(self.y_column_to_set),
        )
        mapper = geography.get_decdeg_to_sweref99tm_mapper(
            data_holder.data["sample_latitude_dd"],
            data_holder.data["sample_longitude_dd"],
            use_db=self._use_db,
        )
        nr = 0
        for (lat, lon), df in data_holder.data.group_by(
            [self.lat_source_col, self.lon_source_col]
        ):
            nr += 1
            if not all([lat, lon]):
                self._log(
                    f"Missing position when trying to set {self.x_column_to_set} "
                    f"and {self.y_column_to_set}"
                )
                continue
            x, y = mapper.get((lat, lon))
            boolean = (pl.col(self.lat_source_col) == lat) & (
                pl.col(self.lon_source_col) == lon
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(boolean)
                .then(pl.lit(x))
                .otherwise(pl.col(self.x_column_to_set))
                .alias(self.x_column_to_set),
                pl.when(boolean)
                .then(pl.lit(y))
                .otherwise(pl.col(self.y_column_to_set))
                .alias(self.y_column_to_set),
            )


class AddSamplePositionDM(Transformer):
    lat_source_col = "sample_latitude_dd"
    lon_source_col = "sample_longitude_dd"
    lat_column_to_set = "sample_latitude_dm"
    lon_column_to_set = "sample_longitude_dm"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sample position in decimal minute"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.lat_column_to_set] = ""
        data_holder.data[self.lon_column_to_set] = ""
        for (lat, lon), df in data_holder.data.groupby(
            [self.lat_source_col, self.lon_source_col]
        ):
            if not all([lat, lon]):
                self._log(
                    f"Missing position when trying to set {self.lat_column_to_set} "
                    f"and {self.lon_column_to_set}"
                )
                continue
            lat = geography.decdeg_to_decmin(lat, with_space=True)
            lon = geography.decdeg_to_decmin(lon, with_space=True)
            data_holder.data.loc[df.index, self.lat_column_to_set] = lat
            data_holder.data.loc[df.index, self.lon_column_to_set] = lon


class PolarsAddReportedPosition(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return (
            "Adds reported position prioritized as follow: "
            "latitude/longitude_deg/min, "
            "sample_reported_-pos, "
            "visit_reported_-pos"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if "latitude_deg" in data_holder.data.columns:
            self._set_reported_from_deg_and_min(data_holder)
        else:
            self._set_reported_from_full_position(data_holder)

    def _set_reported_from_full_position(self, data_holder: PolarsDataHolder):
        self._add_columns_if_missing(data_holder)
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col("sample_reported_latitude") != "")
            .then(pl.col("sample_reported_latitude"))
            .otherwise(pl.col("visit_reported_latitude"))
            .alias("reported_latitude"),
            pl.when(pl.col("sample_reported_longitude") != "")
            .then(pl.col("sample_reported_longitude"))
            .otherwise(pl.col("visit_reported_longitude"))
            .alias("reported_longitude"),
        )

    def _add_columns_if_missing(self, data_holder: PolarsDataHolder):
        cols_to_add = []
        for col in [
            "sample_reported_latitude",
            "sample_reported_longitude",
            "visit_reported_latitude",
            "visit_reported_longitude",
        ]:
            if col in data_holder.data:
                continue
            cols_to_add.append(pl.lit("").alias(col))
        data_holder.data = data_holder.data.with_columns(cols_to_add)

        # data_holder.data = data_holder.data.with_columns(
        #     pl.when(pl.col("sample_reported_latitude") != "")
        #     .then(pl.col("sample_reported_latitude"))
        #     .otherwise(pl.col("visit_reported_latitude"))
        #     .alias("reported_latitude")
        # )
        #
        # data_holder.data = data_holder.data.with_columns(
        #     pl.when(pl.col("sample_reported_longitude") != "")
        #     .then(pl.col("sample_reported_longitude"))
        #     .otherwise(pl.col("visit_reported_longitude"))
        #     .alias("reported_longitude")
        # )

    def _set_reported_from_deg_and_min(self, data_holder: PolarsDataHolder):
        self._save_reported_deg_and_min_columns(data_holder)
        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(
                [
                    pl.col("latitude_deg"),
                    pl.col("latitude_min"),
                ],
                separator=".",
            ).alias("reported_latitude"),
            pl.concat_str(
                [
                    pl.col("longitude_deg"),
                    pl.col("longitude_min"),
                ],
                separator=".",
            ).alias("reported_longitude"),
        )

    def _save_reported_deg_and_min_columns(self, data_holder: PolarsDataHolder):
        for col in [
            "latitude_deg",
            "latitude_min",
            "longitude_deg",
            "longitude_min",
        ]:
            if col not in data_holder.data.columns:
                continue
            reported_col = f"reported_{col}"
            data_holder.data = data_holder.data.with_columns(
                pl.col(col).alias(reported_col)
            )


class PolarsAddSamplePositionDD(PolarsTransformer):
    lat_col_to_set = "sample_latitude_dd"
    lon_col_to_set = "sample_longitude_dd"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.lat_source_col = "reported_latitude"
        self.lon_source_col = "reported_longitude"
        self.lat_nr_sweref_digits = 7
        self.lon_nr_sweref_digits = 6

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sample position based on reported position"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.lat_col_to_set in data_holder.data:
            return
        self._add_empty_col(data_holder, self.lat_col_to_set)
        self._add_empty_col(data_holder, self.lon_col_to_set)
        for (lat, lon), df in data_holder.data.group_by(
            [self.lat_source_col, self.lon_source_col]
        ):
            new_lat, new_lon = self._get(str(lat), str(lon))
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.lat_source_col) == lat)
                .then(pl.lit(new_lat))
                .otherwise(pl.col(self.lat_col_to_set))
                .alias(self.lat_col_to_set)
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.lon_source_col) == lon)
                .then(pl.lit(new_lon))
                .otherwise(pl.col(self.lon_col_to_set))
                .alias(self.lon_col_to_set)
            )

    def _get(self, lat: str, lon: str) -> (str, str):
        lat = lat.replace(" ", "")  # .replace(',', '.')
        lon = lon.replace(" ", "")  # .replace(',', '.')

        if not all([lat, lon]):
            return "", ""

        if self._is_sweref99tm(
            value=lat, nr_digits=self.lat_nr_sweref_digits
        ) and self._is_sweref99tm(value=lon, nr_digits=self.lon_nr_sweref_digits):
            return geography.sweref99tm_to_decdeg(lon, lat)
        elif self._is_dd(lat) and self._is_dd(lon):
            return lat, lon
        elif self._is_dm_lat(lat) and self._is_dm_lon(lon):
            return geography.decmin_to_decdeg(lat), geography.decmin_to_decdeg(lon)
        else:
            self._log(
                f"Unknown format of reported_latitude and/or "
                f"reported_longitude when tying to to set "
                f"{self.lat_source_col} and "
                f"{self.lon_source_col}",
                level=adm_logger.ERROR,
            )
            return "", ""

    def _is_sweref99tm(self, value: str, nr_digits: int) -> bool:
        if len(value.split(".")[0]) == nr_digits:
            return True
        return False

    def _is_dm_lat(self, value: str) -> bool:
        parts = value.split(".")
        if len(parts[0].zfill(4)) == 4:
            return True
        return False

    def _is_dm_lon(self, value: str) -> bool:
        parts = value.split(".")
        if len(parts[0].zfill(5)) == 5:
            return True
        return False

    def _is_dd(self, value: str) -> bool:
        parts = value.split(".")
        if len(parts[0].zfill(2)) == 2:
            return True
        return False


class PolarsAddSamplePositionDM(PolarsTransformer):
    lat_source_col = "sample_latitude_dd"
    lon_source_col = "sample_longitude_dd"
    lat_col_to_set = "sample_latitude_dm"
    lon_col_to_set = "sample_longitude_dm"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sample position in decimal minute"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.lat_col_to_set)
        )
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.lon_col_to_set)
        )
        for (lat, lon), df in data_holder.data.group_by(
            [self.lat_source_col, self.lon_source_col]
        ):
            if not all([lat, lon]):
                self._log(
                    f"Missing position when trying to set {self.lat_col_to_set} "
                    f"and {self.lon_col_to_set}",
                    level=adm_logger.WARNING,
                )
                continue
            new_lat = geography.decdeg_to_decmin(lat, with_space=True)
            new_lon = geography.decdeg_to_decmin(lon, with_space=True)
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.lat_source_col) == lat)
                .then(pl.lit(new_lat))
                .otherwise(pl.col(self.lat_col_to_set))
                .alias(self.lat_col_to_set)
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.lon_source_col) == lon)
                .then(pl.lit(new_lon))
                .otherwise(pl.col(self.lon_col_to_set))
                .alias(self.lon_col_to_set)
            )


class PolarsAddSamplePositionDDAsFloat(PolarsTransformer):
    lat_source_col = "sample_latitude_dd"
    lon_source_col = "sample_longitude_dd"

    lat_col_to_set = "sample_latitude_dd_float"
    lon_col_to_set = "sample_longitude_dd_float"

    def __init__(self, nr_decimals: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._nr_decimals = nr_decimals

    @staticmethod
    def get_transformer_description() -> str:
        return "Creates position_dd columns with float values"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.lat_source_col)
            .cast(float)
            .round(self._nr_decimals)
            .alias(self.lat_col_to_set),
            pl.col(self.lon_source_col)
            .cast(float)
            .round(self._nr_decimals)
            .alias(self.lon_col_to_set),
        )

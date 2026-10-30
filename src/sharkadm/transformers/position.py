import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from sharkadm.utils import decimals, geography, mellifica

from ..data import PolarsDataHolder
from .base import PolarsTransformer


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

    def _transform(self, data_holder: PolarsDataHolder) -> None:
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


class _PolarsPosition(PolarsTransformer):
    lat_col_to_set: str | None = None
    lon_col_to_set: str | None = None

    lat_source_col: str = "reported_latitude"
    lon_source_col: str = "reported_longitude"

    lat_nr_sweref_digits: int = 7
    lon_nr_sweref_digits: int = 6

    @staticmethod
    def get_transformer_description() -> str:
        return "Base class for transformers that add position columns."

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.lat_col_to_set in data_holder.data:
            return
        self._add_empty_col(data_holder, self.lat_col_to_set)
        self._add_empty_col(data_holder, self.lon_col_to_set)
        for (lat, lon), df in data_holder.data.group_by(
            [self.lat_source_col, self.lon_source_col]
        ):
            new_lat, new_lon = self._convert(str(lat), str(lon))
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

    def _get_coordinate_format(self, lat: str, lon: str) -> str | None:
        if self._is_sweref99tm(
            value=lat, nr_digits=self.lat_nr_sweref_digits
        ) and self._is_sweref99tm(value=lon, nr_digits=self.lon_nr_sweref_digits):
            return "sweref99tm"
        elif self._is_sweref99tm(value=lat, nr_digits=7) and self._is_sweref99tm(
            value=lon, nr_digits=7
        ):
            return "sweref99tm"
        elif self._is_dd(lat) and self._is_dd(lon):
            return "DD"
        elif self._is_dm_lat(lat) and self._is_dm_lon(lon):
            return "DM"
        elif self._is_rt90(value=lat) and self._is_rt90(value=lon):
            return "rt90"
        else:
            self._log(
                f"Unknown format of reported_latitude and/or "
                f"reported_longitude when tying to identify the coordinate format of "
                f"{self.lat_source_col} and "
                f"{self.lon_source_col}",
                level=adm_logger.ERROR,
            )
            return None

    def _convert(self, lat: str, lon: str) -> tuple[str, str]:
        return lat, lon

    def _is_sweref99tm(self, value: str, nr_digits: int) -> bool:
        if len(value.split(".")[0]) == nr_digits:
            return True
        return False

    def _is_rt90(self, value: str) -> bool:
        if len(value) == 7:
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


class _PolarsPositionDD(_PolarsPosition):
    lat_col_to_set = "sample_latitude_dd"
    lon_col_to_set = "sample_longitude_dd"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sample position based on reported position"

    def _convert(self, lat: str, lon: str) -> tuple[str, str]:
        lat = lat.replace(" ", "")
        lon = lon.replace(" ", "")

        if not all([lat, lon]):
            self._log(
                f"Missing position when trying to set {self.lat_col_to_set} "
                f"and {self.lon_col_to_set}",
                level=adm_logger.WARNING,
            )
            return "", ""

        reported_format = self._get_coordinate_format(lat, lon)

        if reported_format is None:
            return "", ""
        if reported_format == "DD":
            return lat, lon
        elif reported_format == "DM":
            return (geography.decmin_to_decdeg(lat), geography.decmin_to_decdeg(lon))
        elif reported_format == "sweref99tm":
            return geography.sweref99tm_to_decdeg(lon, lat)
        elif reported_format == "rt90":
            return mellifica.rt90_to_wgs84(float(lon), float(lat))
        else:
            self._log(
                f"Unknown format of reported_latitude and/or "
                f"reported_longitude when tying to to set "
                f"{self.lat_source_col} and "
                f"{self.lon_source_col}",
                level=adm_logger.ERROR,
            )
            return "", ""


class PolarsAddSamplePositionDD(_PolarsPositionDD):
    lat_col_to_set = "sample_latitude_dd"
    lon_col_to_set = "sample_longitude_dd"
    lat_source_col = "reported_latitude"
    lon_source_col = "reported_longitude"
    lat_nr_sweref_digits = 7
    lon_nr_sweref_digits = 6


class PolarsAddVisitTransectEndPositionDD(_PolarsPositionDD):
    valid_data_types = ("ringedseal",)
    lat_col_to_set = "visit_transect_end_latitude_dd"
    lon_col_to_set = "visit_transect_end_longitude_dd"
    lat_source_col = "visit_transect_end_latitude"
    lon_source_col = "visit_transect_end_longitude"
    lat_nr_sweref_digits = 7
    lon_nr_sweref_digits = 6


class PolarsAddVisitTransectStartPositionDD(_PolarsPositionDD):
    valid_data_types = ("ringedseal",)
    lat_col_to_set = "visit_transect_start_latitude_dd"
    lon_col_to_set = "visit_transect_start_longitude_dd"
    lat_source_col = "visit_transect_start_latitude"
    lon_source_col = "visit_transect_start_longitude"
    lat_nr_sweref_digits = 7
    lon_nr_sweref_digits = 6


class PolarsAddSamplePositionDM(_PolarsPosition):
    lat_col_to_set = "sample_latitude_dm"
    lon_col_to_set = "sample_longitude_dm"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds sample position in decimal minute"

    def _convert(self, lat: str, lon: str) -> tuple[str, str]:
        lat = lat.replace(" ", "")
        lon = lon.replace(" ", "")

        if not all([lat, lon]):
            self._log(
                f"Missing position when trying to set {self.lat_col_to_set} "
                f"and {self.lon_col_to_set}",
                level=adm_logger.WARNING,
            )
            return "", ""

        reported_format = self._get_coordinate_format(lat, lon)

        if reported_format == "DM":
            return lat, lon
        elif reported_format == "DD":
            return (geography.decdeg_to_decmin(lat), geography.decdeg_to_decmin(lon))
        elif reported_format == "sweref99tm":
            lat_dd, lon_dd = geography.sweref99tm_to_decdeg(lon, lat)
            return (
                geography.decdeg_to_decmin(lat_dd),
                geography.decdeg_to_decmin(lon_dd),
            )
        elif reported_format == "rt90":
            lat_dd, lon_dd = mellifica.rt90_to_wgs84(float(lon), float(lat))
            return (
                geography.decdeg_to_decmin(lat_dd),
                geography.decdeg_to_decmin(lon_dd),
            )
        else:
            self._log(
                f"Unknown format of reported_latitude and/or "
                f"reported_longitude when tying to to set "
                f"{self.lat_source_col} and "
                f"{self.lon_source_col}",
                level=adm_logger.ERROR,
            )
            return "", ""


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


class PolarsSetPositionDDNumberOfDecimal(PolarsTransformer):
    lat_col = "sample_latitude_dd"
    lon_col = "sample_longitude_dd"

    def __init__(self, nr_decimals: int = 2, pad_end: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._nr_decimals = nr_decimals
        self._pad_end = pad_end

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets nr decimals to {PolarsSetPositionDDNumberOfDecimal.lat_col} and "
            f"{PolarsSetPositionDDNumberOfDecimal.lon_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for col in [self.lat_col, self.lon_col]:
            data_holder.data = decimals.set_nr_decimals(
                data_holder.data,
                col=col,
                nr_decimals=self._nr_decimals,
                pad_end=self._pad_end,
            )


class PolarsAddReportedPositionString(PolarsTransformer):
    lat_source_col = "reported_latitude"
    lon_source_col = "reported_longitude"
    col_to_set = "reported_position_str"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Creates a concatenated position column "
            f"named {PolarsAddReportedPositionString.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(
                [
                    pl.col(self.lat_source_col),
                    pl.col(self.lon_source_col),
                ],
                separator="_",
            ).alias(self.col_to_set),
        )


class PolarsAddPositionDirection(PolarsTransformer):
    lat_source_col = "sample_latitude_dd"
    lon_source_col = "sample_longitude_dd"
    lat_col_to_set = "lat_direction"
    lon_col_to_set = "lon_direction"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds column with position direction [N, E, S, W]"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(self.lat_source_col).cast(float, strict=False) < 0)
            .then(pl.lit("S"))
            .otherwise(pl.lit("N"))
            .alias(self.lat_col_to_set),
            pl.when(pl.col(self.lon_source_col).cast(float, strict=False) < 0)
            .then(pl.lit("W"))
            .otherwise(pl.lit("E"))
            .alias(self.lon_col_to_set),
        )


class PolarsAddPositionDDId(PolarsTransformer):
    col_to_set = "position_dd_id"

    def __init__(self, separator: str = "", **kwargs):
        super().__init__(**kwargs)
        self._separator = separator

    @staticmethod
    def get_transformer_description() -> str:
        return f"Creates a {PolarsAddPositionDDId.col_to_set} as in old Java code"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(
                [
                    pl.col("lat_direction"),
                    pl.col("sample_latitude_dd").cast(float).abs().round(5).cast(str),
                    pl.lit(self._separator),
                    pl.col("lon_direction"),
                    pl.col("sample_longitude_dd").cast(float).abs().round(4).cast(str),
                ],
                separator="",
            ).alias(self.col_to_set)
        )


class PolarsAddSpaceInPositionDM(PolarsTransformer):
    lat_source_col = "sample_latitude_dm"
    lon_source_col = "sample_longitude_dm"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds space in {PolarsAddSpaceInPositionDM.lat_source_col} and "
            f"{PolarsAddSpaceInPositionDM.lon_source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for col in [self.lat_source_col, self.lon_source_col]:
            if col not in data_holder.data.columns:
                self._log(
                    f"Missing column {col} when trying to insert space",
                    level=adm_logger.DEBUG,
                )
                continue
            data_holder.data = data_holder.data.with_columns(
                pl.col(col).str.replace(r"^(\d{2})(.*)$", r"${1} ${2}")
            )

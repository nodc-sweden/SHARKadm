import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import DataHolderProtocol, PolarsTransformer, Transformer

try:
    from nodc_station import get_station_object
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class AddStationInfo(Transformer):
    source_lat_column = "sample_latitude_dd"
    source_lon_column = "sample_longitude_dd"
    reported_station_col = "reported_station_name"
    columns_to_set = (
        "station_name",
        "station_id",
        "sample_location_id",
        "station_viss_eu_id",
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._stations = get_station_object()
        self._station_synonyms = {}
        self._loaded_stations_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds station information to all places"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._create_columns_if_missing(data_holder)

        for (lat_str, lon_str, reported_station), df in data_holder.data.groupby(
            [self.source_lat_column, self.source_lon_column, self.reported_station_col]
        ):
            if not (lat_str and lon_str):
                rows = ", ".join(list(df["row_number"]))
                self._log(
                    adm_logger.feedback.missing_position(
                        rows=sorted(set(df["row_number"]))
                    ),
                    level=adm_logger.ERROR,
                    purpose=adm_logger.FEEDBACK,
                )  # Ska kanske vara i validator istället
                self._log(
                    f"Missing {self.source_lat_column} and/or {self.source_lon_column} "
                    f"in {self.__class__.__name__}",
                    row_number=rows,
                    level=adm_logger.ERROR,
                )
                continue

            boolean = (
                (data_holder.data[self.source_lat_column] == lat_str)
                & (data_holder.data[self.source_lon_column] == lon_str)
                & (data_holder.data[self.reported_station_col] == reported_station)
            )

            lat = float(lat_str)
            lon = float(lon_str)

            info = self._loaded_stations_info.setdefault(
                reported_station,
                self._stations.get_station_info_from_synonym_and_position(
                    reported_station, lat, lon
                ),
            )
            if not info:
                closest_info = self._stations.get_closest_station_info(lat, lon)
                if len(closest_info) == 1:
                    closest_info = closest_info[0]
                    if closest_info["accepted"]:
                        self._log(
                            f"Station '{reported_station}' is not found as a synonym in "
                            f"station list Closest station is "
                            f"{closest_info['STATION_NAME']} and is accepted",
                            level=adm_logger.WARNING,
                        )
                    else:
                        self._log(
                            f"Station '{reported_station}' is not found as a synonym in "
                            f"station list Closest station is "
                            f"{closest_info['STATION_NAME']} but is not accepted",
                            level=adm_logger.WARNING,
                        )
                    continue
                else:
                    station_names_str = ", ".join(
                        [info["STATION_NAME"] for info in closest_info]
                    )
                    self._log(
                        f"Station '{reported_station}' is not found as a synonym in "
                        f"station list. Closest station(s) is/are {station_names_str}",
                        level=adm_logger.WARNING,
                    )
                    continue
            if not info["accepted"]:
                self._log(
                    f"Reported station name found in station list but it is outside "
                    f"the accepted radius. Distance={info['calc_dist']}, "
                    f"Accepted radius={info['OUT_OF_BOUNDS_RADIUS']}",
                    level=adm_logger.WARNING,
                )
                adm_logger.log_validation_failed(
                    f"Reported station name found in station list but it is outside "
                    f"the accepted radius. Distance={info['calc_dist']}, "
                    f"Accepted radius={info['OUT_OF_BOUNDS_RADIUS']}",
                    level=adm_logger.WARNING,
                )
                continue

            if reported_station != info["STATION_NAME"]:
                name = info["STATION_NAME"]
                self._log(
                    f"Station name translated: {reported_station} -> {name}",
                    level="warning",
                )
            data_holder.data.loc[boolean, "station_name"] = info["STATION_NAME"]
            data_holder.data.loc[boolean, "station_id"] = info["REG_ID_GROUP"]
            data_holder.data.loc[boolean, "sample_location_id"] = info["REG_ID"]
            data_holder.data.loc[boolean, "station_viss_eu_id"] = info["EU_CD"]
            adm_logger.log_validation_succeeded(
                f"Station '{info['STATION_NAME']}' ({info['REG_ID_GROUP']}) "
                f"transformed without error.",
                level=adm_logger.INFO,
            )

    def _create_columns_if_missing(self, data_holder: DataHolderProtocol) -> None:
        for col in self.columns_to_set:
            if col not in data_holder.data.columns:
                self._log(f"Adding column {col}", level=adm_logger.DEBUG)
                data_holder.data[col] = ""


class CopyReportedStationNameToStationName(Transformer):
    valid_data_types = ("plankton_imaging",)

    source_column = "reported_station_name"
    col_to_set = "station_name"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Copies {CopyReportedStationNameToStationName.source_column} to "
            f"{CopyReportedStationNameToStationName.col_to_set}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_column]


class PolarsCopyReportedStationNameToStationName(PolarsTransformer):
    valid_data_types = ("plankton_imaging",)

    source_column = "reported_station_name"
    col_to_set = "station_name"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Copies {PolarsCopyReportedStationNameToStationName.source_column} to "
            f"{PolarsCopyReportedStationNameToStationName.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )


class PolarsAddStationInfo(PolarsTransformer):
    source_lat_column = "sample_latitude_dd"
    source_lon_column = "sample_longitude_dd"
    reported_station_col = "reported_station_name"
    columns_to_set = (
        "station_name",
        "station_id",
        "sample_location_id",
        "station_viss_eu_id",
        "distance_to_matched_station",
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._stations = get_station_object()
        self._station_synonyms = {}
        self._loaded_stations_info = {}

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds station information to all places"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._create_columns_if_missing(data_holder)

        for (lat_str, lon_str, reported_station), df in data_holder.data.group_by(
            [self.source_lat_column, self.source_lon_column, self.reported_station_col]
        ):
            if not (lat_str and lon_str):
                rows = list(df["row_number"])
                self._log(
                    adm_logger.feedback.missing_position(
                        rows=sorted(set(df["row_number"]))
                    ),
                    level=adm_logger.ERROR,
                    purpose=adm_logger.FEEDBACK,
                )  # Ska kanske vara i validator istället
                self._log(
                    f"Missing {self.source_lat_column} and/or {self.source_lon_column}",
                    row_numbers=rows,
                    level=adm_logger.ERROR,
                )
                continue

            lat = float(lat_str)
            lon = float(lon_str)

            matching_stations = self._stations.get_matching_stations(
                name=reported_station, lat_dd=lat, lon_dd=lon
            )
            if not matching_stations:
                self._log(
                    f"No matching station found for station {reported_station} "
                    f"at position {lat}:{lon}",
                    level=adm_logger.WARNING,
                )
                continue

            accepted_station = matching_stations.get_accepted_station()
            if not accepted_station:
                closest_station = matching_stations.get_closest_station()
                synonym_station = matching_stations.get_accepted_station_by_name_only()
                distance = f"{closest_station.distance:,}".replace(",", " ")
                msg = (
                    f"No accepted station found for station {reported_station} "
                    f"at position {lat} : {lon}. "
                    f"Closest station is {closest_station.station} "
                    f"({distance} m)."
                )
                if synonym_station:
                    distance = f"{synonym_station.distance:,}".replace(",", " ")
                    msg = (
                        f"{msg} Synonym matches station {synonym_station.station} "
                        f"({distance} m)"
                    )
                self._log(
                    msg,
                    level=adm_logger.WARNING,
                )
                continue

            if reported_station != accepted_station.station:
                self._log(
                    f"Station name translated: "
                    f"{reported_station} -> {accepted_station.station}",
                    level=adm_logger.INFO,
                )

            condition = (
                (pl.col(self.reported_station_col) == reported_station)
                & (pl.col(self.source_lat_column) == lat_str)
                & (pl.col(self.source_lon_column) == lon_str)
            )

            data_holder.data = data_holder.data.with_columns(
                [
                    pl.when(condition)
                    .then(pl.lit(accepted_station.station))
                    .otherwise(pl.col("station_name"))
                    .alias("station_name"),
                    pl.when(condition)
                    .then(pl.lit(accepted_station.reg_id_group))
                    .otherwise(pl.col("station_id"))
                    .alias("station_id"),
                    pl.when(condition)
                    .then(pl.lit(accepted_station.reg_id))
                    .otherwise(pl.col("sample_location_id"))
                    .alias("sample_location_id"),
                    pl.when(condition)
                    .then(pl.lit(accepted_station.eu_cd))
                    .otherwise(pl.col("station_viss_eu_id"))
                    .alias("station_viss_eu_id"),
                    pl.when(condition)
                    .then(pl.lit(accepted_station.distance))
                    .otherwise(pl.col("distance_to_matched_station"))
                    .alias("distance_to_matched_station"),
                ]
            )

    def _create_columns_if_missing(self, data_holder: PolarsDataHolder) -> None:
        for col in self.columns_to_set:
            if col in data_holder.data.columns:
                continue
            self._log(f"Adding column {col}", level=adm_logger.DEBUG)
            self._add_empty_col(data_holder, col)

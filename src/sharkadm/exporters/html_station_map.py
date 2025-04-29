import pathlib

import geopandas
import pandas as pd
import polars as pl
import numpy as np

from sharkadm.sharkadm_logger import adm_logger
from ..data import PolarsDataHolder

try:
    from nodc_station import DEFAULT_STATION_FILE_PATH
    from nodc_station.main import App
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. You need to '
        f"install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )

folium = None
try:
    import folium
    import geopandas as gpd
    import shapely
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. You need to '
        f"install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )

nodc_geography = None
try:
    import nodc_geography
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. You need to '
        f"install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


from .base import DataHolderProtocol, FileExporter


class HtmlStationMap(FileExporter):
    """Creates a html station map"""

    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__(export_directory, export_file_name, **kwargs)

    def _get_path(self, data_holder: DataHolderProtocol) -> pathlib.Path:
        if not self._export_file_name:
            self._export_file_name = f"station_map_{data_holder.dataset_name}.html"
        path = pathlib.Path(self._export_directory, self._export_file_name)
        if path.suffix != ".html":
            path = pathlib.Path(str(path) + ".html")
        return path

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a html station map."

    def _export(self, data_holder: DataHolderProtocol) -> None:
        app = App()

        # Read master list
        app.read_list(
            DEFAULT_STATION_FILE_PATH, reader="shark_master", list_name="master"
        )

        list_name = "sharkadm_data"
        df = self._get_position_dataframe(data_holder.data)
        if df.empty:
            adm_logger.log_export("No data to plot html map", level=adm_logger.WARNING)
            return
        app.add_list_data(df, list_name=list_name)

        export_path = self._get_path(data_holder)

        app.write_list(
            writer="map",
            # list_names=['master'],
            # list_names=['stnreg_import'],
            list_names=["master", list_name],
            new_stations_as_cluster=False,
            file_path=str(export_path),
        )

    def _get_position_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Removes duplicated positions in dataframe"""
        unique_df = df.drop_duplicates(
            subset=["sample_latitude_dd", "sample_longitude_dd"], keep="last"
        ).reset_index(drop=True)
        remove_boolean = (unique_df["sample_latitude_dd"] == "") | (
            unique_df["sample_longitude_dd"] == ""
        )
        dframe = unique_df[~remove_boolean]
        dframe["lat_dd"] = dframe["sample_latitude_dd"]
        dframe["lon_dd"] = dframe["sample_longitude_dd"]
        return dframe.reset_index(drop=True)


class PolarsHtmlStationMap(FileExporter):
    """Creates a html station map"""

    def __init__(
        self,
        load_station_list: bool = False,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        self._load_station_list = load_station_list
        super().__init__(export_directory, export_file_name, **kwargs)

    def _get_path(self, data_holder: DataHolderProtocol) -> pathlib.Path:
        if not self._export_file_name:
            self._export_file_name = f"station_map_{data_holder.dataset_name}.html"
        path = pathlib.Path(self._export_directory, self._export_file_name)
        if path.suffix != ".html":
            path = pathlib.Path(str(path) + ".html")
        return path

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a html station map."

    def _export(self, data_holder: DataHolderProtocol) -> None:
        app = App()

        list_name = "sharkadm_data"
        list_names = [list_name]
        if self._load_station_list:
            # Read master list
            app.read_list(
                DEFAULT_STATION_FILE_PATH, reader="shark_master", list_name="master"
            )
            list_names = ["master", *list_names]

        df = self._get_position_dataframe(data_holder.data)
        if df.empty:
            adm_logger.log_export("No data to plot html map", level=adm_logger.WARNING)
            return
        app.add_list_data(df, list_name=list_name)

        export_path = self._get_path(data_holder)

        app.write_list(
            writer="map",
            # list_names=['master'],
            # list_names=['stnreg_import'],
            list_names=list_names,
            new_stations_as_cluster=False,
            file_path=str(export_path),
        )

    def _get_position_dataframe(self, df: pl.DataFrame) -> pd.DataFrame:
        """Removes duplicated positions in dataframe"""
        pandas_df = df.to_pandas()
        unique_df = pandas_df.drop_duplicates(
            subset=["sample_latitude_dd", "sample_longitude_dd"], keep="last"
        ).reset_index(drop=True)
        remove_boolean = (unique_df["sample_latitude_dd"] == "") | (
            unique_df["sample_longitude_dd"] == ""
        )
        dframe = unique_df[~remove_boolean]
        dframe["lat_dd"] = dframe["sample_latitude_dd"]
        dframe["lon_dd"] = dframe["sample_longitude_dd"]
        return dframe.reset_index(drop=True)


class PolarsHtmlMap(FileExporter):
    shape_layers: tuple[str, ...] = ()

    def __init__(
        self,
        # load_station_list: bool = False,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        shape_layers: list[str | pathlib.Path] = None,
        shape_files: list[str | pathlib.Path] = None,
        highlight_stations_in_areas: bool = False,
        columns_to_show: list[str] = None,
        **kwargs,
    ):
        # self._load_station_list = load_station_list
        self._shape_layers = self.shape_layers or shape_layers or []
        self._shape_files = shape_files or []
        self._highlight_stations_in_areas = highlight_stations_in_areas
        self._epsg = kwargs.get("epsg", "3006")
        self._station_info = []
        self._gdfs = []
        self._points_in_areas = []
        self._columns_to_show = columns_to_show or []
        super().__init__(export_directory, export_file_name, **kwargs)

    def _get_path(self, data_holder: PolarsDataHolder) -> pathlib.Path:
        if not self._export_file_name:
            self._export_file_name = f"station_map_{data_holder.dataset_name}.html"
        path = pathlib.Path(self._export_directory, self._export_file_name)
        if path.suffix != ".html":
            path = pathlib.Path(str(path) + ".html")
        return path

    @staticmethod
    def get_exporter_description() -> str:
        return "Creates a html map with markers. Option to add shape-files"

    def _export(self, data_holder: PolarsDataHolder) -> None:
        if not folium:
            adm_logger.log_export(f"Could not export {self.__class__.__name__}. "
                                  f"folium package not found!", level=adm_logger.WARNING)
            return

        lat_mid = np.mean([data_holder.data['sample_latitude_dd'].cast(float).max(),
                           data_holder.data['sample_latitude_dd'].cast(float).min()])
        lon_mid = np.mean([data_holder.data['sample_longitude_dd'].cast(float).max(),
                           data_holder.data['sample_longitude_dd'].cast(float).min()])

        m = folium.Map(location=(float(lat_mid), float(lon_mid)), zoom_start=6)
        # folium.TileLayer("OpenStreetMap").add_to(m)
        # folium.TileLayer(show=False).add_to(m)
        self._add_shape_layers(m)
        self._add_shape_files(m)
        self._save_station_info(data_holder)
        self._check_paints_in_areas()
        self._add_markers(m)
        folium.LayerControl().add_to(m)

        export_path = self._get_path(data_holder)
        m.save(export_path)

    def _add_shape_layers(self, m: "folium.Map") -> None:
        if not self._shape_layers:
            return
        if not nodc_geography:
            adm_logger.log_export(f"Could not add shape layers to {self.__class__.__name__}. "
                                  f"Package nodc_qeography not found!", level=adm_logger.WARNING)
            return
        for layer in self._shape_layers:
            shape = nodc_geography._get_shapefile_for_variable(layer)
            if not shape:
                adm_logger.log_export(
                    f"No shape file found for layer {layer}", level=adm_logger.WARNING)
                continue
            fg = folium.FeatureGroup(name=layer, show=True)
            folium.GeoJson(data=shape.gdf).add_to(fg)
            fg.add_to(m)
            self._gdfs.append(shape.gdf)

    def _add_shape_files(self, m: "folium.Map") -> None:
        if not self._shape_files:
            return
        for item in self._shape_files:
            path = pathlib.Path(item)
            gdf = gpd.read_file(path)
            gdf.crs = self._epsg
            fg = folium.FeatureGroup(name=path.stem, show=True)
            folium.GeoJson(data=gdf).add_to(fg)
            fg.add_to(m)
            self._gdfs.append(gdf)

    def _save_station_info(self, data_holder: PolarsDataHolder):
        cols_to_show = [col for col in self._columns_to_show if col in data_holder.data]
        for (lat, lon, x, y, station), df in data_holder.data.group_by(["sample_latitude_dd",
                                                                  "sample_longitude_dd",
                                                                  "sample_sweref99tm_x",
                                                                  "sample_sweref99tm_y",
                                                                  "reported_station_name"]):
            items = [station, f"Latitude: {lat}", f"Longitude: {lon}"]
            for col in cols_to_show:
                items.append(f"{col}: {df.item(0, col)}")
            text = "\n".join(items)

            self._station_info.append(
                dict(
                    lat=float(lat),
                    lon=float(lon),
                    x=float(x),
                    y=float(y),
                    station=station,
                    text=text,
                    in_areas=False
                )
            )

    def _check_paints_in_areas(self):
        if not self._highlight_stations_in_areas:
            return
        if not self._gdfs:
            return
        for info in self._station_info:
            if not info.get('x'):
                adm_logger.log_export(f'Could not highlight stations in areas. Missing column sample_sweref99tm_x', level=adm_logger.ERROR)
                raise KeyError('sample_sweref99tm_x and sample_sweref99tm_y')
            for gdf in self._gdfs:
                if sum(gdf.contains(shapely.Point(info["x"], info["y"]))):
                    info["in_areas"] = True
                    break

    def _add_markers(self, m: "folium.Map") -> None:
        fg = folium.FeatureGroup(name="Stations", show=True)
        for info in self._station_info:
            color = 'blue'
            if info.get('in_areas'):
                color='red'
            folium.Marker(
                location=[info["lat"], info["lon"]],
                tooltip=info["station"],
                popup=info["text"],
                icon=folium.Icon(color=color),
            ).add_to(fg)
        fg.add_to(m)


class PolarsHtmlMapR(PolarsHtmlMap):
    shape_layers: tuple[str, ...] = (
        "location_ra",
        "location_rb",
        "location_rc",
        "location_rg",
        "location_rh",
        "location_ro",
    )

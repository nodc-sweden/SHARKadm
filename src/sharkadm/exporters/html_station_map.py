import pathlib
from enum import StrEnum

import numpy as np
import pandas as pd
import polars as pl

from sharkadm.sharkadm_logger import adm_logger
from .base import DataHolderProtocol, FileExporter
from ..data import PolarsDataHolder

POPUP_WIDTH = 300
POPUP_HEIGHT = 400


class MarkerColor(StrEnum):
    NOT_ACCEPTED = "orange"
    ACCEPTED = "green"
    MASTER = "blue"
    MASTER_MATCH = "darkblue"
    IN_SHAPE = "pink"
    ON_LAND = "red"
    MANUAL_STATION = "blue"


class AreaColor(StrEnum):
    BUFFER = "gray"
    MASTER_RADIUS = "blue"
    MASTER_RADIUS_MATCH = "darkblue"
    MANUAL_RADIUS = '#3186cc'


nodc_station = None
try:
    import nodc_station
    from nodc_station.station_file import MatchingStation
    from nodc_station import DEFAULT_STATION_FILE_PATH
    # from nodc_station.main import App
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


def get_popup_html_table(title: str, data: dict) -> str:
    lines = [f"""
        <body style="font-family:Verdana; "></body>
        <h1 style="font-size:14px; font-weight:bold">{title}</h1>
        <table style="font-size:12px;border-collapse:collapse;">
        """]

    for key, value in data.items():
        if isinstance(value, list):
            value = "<br>".join(value)
        lines.append(f"""
        <tr style="vertical-align:top; border:solid; border-width: 1px 0;">
            <td style="font-weight:bold">{key}</td>
            <td style="padding-inline:20px 5px">{value}</td>
        </tr>
        """)

    lines.append("""
        </table>
        """)

    html = "\n".join(lines)
    return html


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
            self._log("No data to plot html map", level=adm_logger.WARNING)
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
            self._log("No data to plot html map", level=adm_logger.WARNING)
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
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        shape_layers: list[str | pathlib.Path] | None = None,
        shape_files: list[str | pathlib.Path] | None = None,
        highlight_stations_in_areas: bool = False,
        columns_to_show: list[str] | None = None,
        show_master_stations_within_radius: int | bool = 10_000,
        show_accepted: bool = True,
        show_stations: list[str] = None,
        **kwargs,
    ):
        self._shape_layers = self.shape_layers or shape_layers or []
        self._shape_files = shape_files or []
        self._highlight_stations_in_areas = highlight_stations_in_areas
        self._epsg = kwargs.get("epsg", "3006")
        self._station_info = []
        self._matching_stations_by_reg_id = {}
        self._master_station_info = []
        self._other_stations_info = []
        self._gdfs = []
        self._points_in_areas = []
        self._columns_to_show = columns_to_show or []
        self._show_master_stations_within_radius = show_master_stations_within_radius
        self._show_accepted = show_accepted
        self._show_stations = show_stations
        super().__init__(export_directory, export_file_name, **kwargs)

        self._master = nodc_station.get_station_object()

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
            self._log(
                f"Could not export {self.__class__.__name__}. folium package not found!",
                level=adm_logger.WARNING,
            )
            return

        lat_mid, lon_mid = self._get_mid_position_for_data(data_holder.data)
        m = folium.Map(location=(lat_mid, lon_mid), zoom_start=6, tiles="Cartodb Positron")

        # folium.TileLayer("OpenStreetMap").add_to(m)
        # folium.TileLayer(show=False).add_to(m)
        self._add_shape_layers(m)
        self._add_shape_files(m)
        self._save_station_info(data_holder)
        self._save_master_station_info(data_holder)
        self._save_other_stations_info()

        self._check_points_in_areas()
        if self._show_master_stations_within_radius:
            self._add_master_markers(m)
            self._add_marker_master_radius(m)
            self._add_marker_buffer(m)
        self._add_other_station_markers(m)
        self._add_other_marker_radius(m)
        self._add_markers(m)
        folium.LayerControl().add_to(m)

        export_path = self._get_path(data_holder)
        m.save(export_path)

    @staticmethod
    def _get_mid_position_for_data(data: pl.DataFrame) -> (float, float):
        lat_mid = np.mean(
            [
                data["sample_latitude_dd"].cast(float).max(),
                data["sample_latitude_dd"].cast(float).min(),
            ]
        )
        lon_mid = np.mean(
            [
                data["sample_longitude_dd"].cast(float).max(),
                data["sample_longitude_dd"].cast(float).min(),
            ]
        )
        return lat_mid, lon_mid

    def _add_shape_layers(self, m: "folium.Map") -> None:
        if not self._shape_layers:
            return
        if not nodc_geography:
            self._log(
                f"Could not add shape layers to {self.__class__.__name__}. "
                f"Package nodc_qeography not found!",
                level=adm_logger.WARNING,
            )
            return
        for layer in self._shape_layers:
            shape = nodc_geography._get_shapefile_for_variable(layer)
            if not shape:
                self._log(
                    f"No shape file found for layer {layer}", level=adm_logger.WARNING
                )
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
        red_df = data_holder.data.unique([
            "sample_latitude_dd",
            "sample_longitude_dd",
        ])
        for info in red_df.to_dicts():
            accepted = False
            if self._show_accepted:
                all_matching = self._master.get_matching_stations(
                    info["reported_station_name"],
                    info["sample_latitude_dd"],
                    info["sample_longitude_dd"],
                )
                matching: MatchingStation | None = all_matching.get_accepted_station()
                popup_data = {}
                if matching:
                    accepted = True
                    popup_data = {
                        "Matchat stationsnamn": matching.station,
                        "Provplats ID": matching.reg_id,
                        "Synonymer": matching.synonyms,
                        "Avstånd till matchad station": f"{matching.distance} m",
                    }
                    self._matching_stations_by_reg_id[matching.reg_id] = matching

                popup_data.update({
                    "Latitude DD": info["sample_latitude_dd"],
                    "Longitude DD": info["sample_longitude_dd"],
                    "Latitude DM": info["sample_latitude_dm"],
                    "Longitude DM": info["sample_longitude_dm"],
                    "sweref99tm_y": round(float(info["sample_sweref99tm_y"])),
                    "sweref99tm_x": round(float(info["sample_sweref99tm_x"])),
                })

                for col in cols_to_show:
                    popup_data[col] = info[col]

                html = get_popup_html_table(
                    title=info["reported_station_name"],
                    data=popup_data
                )

                self._station_info.append(
                    dict(
                        lat_dd=float(info["sample_latitude_dd"]),
                        lon_dd=float(info["sample_longitude_dd"]),
                        sweref99tm_x=float(info["sample_sweref99tm_x"]),
                        sweref99tm_y=float(info["sample_sweref99tm_y"]),
                        station_name=info["reported_station_name"],
                        popup_html=html,
                        in_areas=False,
                        accepted=accepted,
                        matching_station=matching,
                    )
                )

    def _save_master_station_info(self, data_holder: PolarsDataHolder):
        if not self._show_master_stations_within_radius:
            return

        if self._show_master_stations_within_radius is True:
            all_df = self._master.gdf.copy()
        else:
            all_df = self._master.get_stations_within_buffer(data_holder.data,
                                                   self._show_master_stations_within_radius)

        station_infos = all_df.to_dict(orient="records")

        for info in station_infos:
            html = get_popup_html_table(
                title=info['station_name'],
                data={
                    "Provplats ID": info["reg_id"],
                    "Synonymer": info["synonyms"],
                    "Radie": info["radius"],
                    "Latitude DD": info["lat_dd"],
                    "Longitude DD": info["lon_dd"],
                    "Latitude DM": info["lat_dm"],
                    "Longitude DM:": info["lon_dm"],
                    "sweref99tm_y": info["sweref99tm_y"],
                    "sweref99tm_x": info["sweref99tm_x"],
                }
            )
            info["popup_html"] = html
            self._master_station_info.append(info)

    def _save_other_stations_info(self):
        if not self._show_stations:
            return
        df = self._master.get_stations_by_name(self._show_stations)
        station_infos = df.to_dict(orient="records")
        for info in station_infos:
            html = get_popup_html_table(
                title=info['station_name'],
                data={
                    "Provplats ID": info["reg_id"],
                    "Synonymer": info["synonyms"],
                    "Radie": info["radius"],
                    "Latitude DD": info["lat_dd"],
                    "Longitude DD": info["lon_dd"],
                    "Latitude DM": info["lat_dm"],
                    "Longitude DM:": info["lon_dm"],
                    "sweref99tm_y": info["sweref99tm_y"],
                    "sweref99tm_x": info["sweref99tm_x"],
                }
            )
            info["popup_html"] = html
            self._other_stations_info.append(info)

    def _check_points_in_areas(self):
        if not self._highlight_stations_in_areas:
            return
        if not self._gdfs:
            return
        for info in self._station_info:
            if not info.get("x"):
                self._log(
                    "Could not highlight stations in areas. "
                    "Missing column sample_sweref99tm_x",
                    level=adm_logger.ERROR,
                )
                raise KeyError("sample_sweref99tm_x and sample_sweref99tm_y")
            for gdf in self._gdfs:
                if sum(gdf.contains(shapely.Point(info["x"], info["y"]))):
                    info["in_areas"] = True
                    break

    def _add_markers(self, m: "folium.Map") -> None:
        fg = folium.FeatureGroup(name="Stations", show=True)
        for info in self._station_info:
            color = MarkerColor.NOT_ACCEPTED
            if info.get("accepted"):
                color = MarkerColor.ACCEPTED
            elif info.get("in_areas"):
                color = MarkerColor.IN_SHAPE
            iframe = folium.IFrame(info["popup_html"],
                                   # width=POPUP_WIDTH,
                                   # height=POPUP_HEIGHT
                                   )
            popup = folium.Popup(iframe, lazy=True,
                                 min_width=POPUP_WIDTH,
                                 max_width=POPUP_WIDTH,
                                 # min_height=POPUP_HEIGHT,
                                 # max_height=POPUP_HEIGHT,
                                 )
            folium.Marker(
                location=[info["lat_dd"], info["lon_dd"]],
                tooltip=info["station_name"],
                popup=popup,
                icon=folium.Icon(color=color),
            ).add_to(fg)
        fg.add_to(m)

    def _add_marker_buffer(self, m: "folium.Map"):
        if not self._show_master_stations_within_radius:
            return
        fg = folium.FeatureGroup(name="Station buffer", show=False)
        for info in self._station_info:
            print(sorted(info))
            folium.Circle(
                location=[info["lat_dd"],
                          info["lon_dd"]],
                tooltip=f"Radius: "
                        f"{self._show_master_stations_within_radius:_} m".replace("_", " "),
                # popup=f"Radius: {self._show_master_stations_within_radius} m",
                radius=self._show_master_stations_within_radius,
                fill_color=AreaColor.BUFFER,
                weight=.5,
                ).add_to(fg)
            # folium.Marker(
            #     location=[info["lat"], info["lon"]],
            #     tooltip=info["station"],
            #     popup=info["text"],
            #     icon=folium.Icon(color=color),
            # ).add_to(fg)
        fg.add_to(m)

    def _add_master_markers(self, m: "folium.Map") -> None:
        if not self._master_station_info:
            return
        fg = folium.FeatureGroup(name="Master stations", show=True)
        for info in self._master_station_info:
            color = MarkerColor.MASTER
            if self._matching_stations_by_reg_id.get(info["reg_id"]):
                color = MarkerColor.MASTER_MATCH
            iframe = folium.IFrame(info["popup_html"],
                                   width=POPUP_WIDTH,
                                   height=POPUP_HEIGHT)
            popup = folium.Popup(iframe, lazy=True, min_width=POPUP_WIDTH,
                                 max_width=POPUP_WIDTH,
                                 )
            folium.Marker(
                location=[info["lat_dd"], info["lon_dd"]],
                tooltip=info["station_name"],
                popup=popup,
                # popup=info["text"],
                icon=folium.Icon(color=color),
            ).add_to(fg)
        fg.add_to(m)

    def _add_other_station_markers(self, m: "folium.Map") -> None:
        if not self._other_stations_info:
            return
        fg = folium.FeatureGroup(name="Other stations", show=True)
        for info in self._other_stations_info:
            color = MarkerColor.MANUAL_STATION
            iframe = folium.IFrame(info["popup_html"],
                                   width=POPUP_WIDTH,
                                   height=POPUP_HEIGHT)
            popup = folium.Popup(iframe, lazy=True, min_width=POPUP_WIDTH,
                                 max_width=POPUP_WIDTH,
                                 )
            folium.Marker(
                location=[info["lat_dd"], info["lon_dd"]],
                tooltip=info["station_name"],
                popup=popup,
                icon=folium.Icon(color=color),
            ).add_to(fg)
        fg.add_to(m)

    def _add_marker_master_radius(self, m: "folium.Map"):
        fg = folium.FeatureGroup(name="Master station radius", show=True)
        for info in self._master_station_info:
            color = AreaColor.MASTER_RADIUS
            if self._matching_stations_by_reg_id.get(info["reg_id"]):
                color = AreaColor.MASTER_RADIUS_MATCH
            folium.Circle(
                location=[info["lat_dd"], info["lon_dd"]],
                tooltip=f"Radius: {info['radius']:_} m".replace("_", " "),
                radius=int(info["radius"]),
                fill_color=color,
                weight=.5,
                ).add_to(fg)
        fg.add_to(m)

    def _add_other_marker_radius(self, m: "folium.Map"):
        if not self._other_stations_info:
            return
        fg = folium.FeatureGroup(name="Other station radius", show=True)
        for info in self._other_stations_info:
            folium.Circle(
                location=[info["lat_dd"], info["lon_dd"]],
                tooltip=f"Radius: {info['radius']:_} m".replace("_", " "),
                radius=int(info["radius"]),
                fill_color=AreaColor.MANUAL_RADIUS,
                weight=.5,
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


class PolarsHtmlMapRred(PolarsHtmlMap):
    shape_layers: tuple[str, ...] = (
        "location_rc",
        "location_rg",
        "location_ro",
    )

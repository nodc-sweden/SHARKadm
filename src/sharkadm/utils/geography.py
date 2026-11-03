import numpy as np
import pyproj

from sharkadm.utils import sweref99tm_db


def decdeg_to_decmin(
    pos: float | str, nr_decimals: int | None = None, with_space: bool = False
):
    pos = float(pos)
    deg = np.floor(pos)
    minute = 0
    if deg:
        minute = pos % deg * 60.0
    if nr_decimals is not None:
        output = "%%2.%sf" % nr_decimals % (deg * 100.0 + minute)
    else:
        output = str(deg * 100.0 + minute)
    if with_space:
        a, b = output.split(".")
        output = f"{a[:-2]} {a[-2:]}.{b}"
    return output


def decmin_to_decdeg(pos: str, nr_decimals: int | None = None):
    pos = float(pos.replace(" ", ""))
    if pos >= 0:
        output = np.floor(pos / 100.0) + (pos % 100) / 60.0
    else:
        output = np.ceil(pos / 100.0) - (-pos % 100) / 60.0

    if nr_decimals is not None:
        output = np.round(output, nr_decimals)

    return str(output)


def wgs84_to_sweref99tm(lat: str, lon: str, use_db: bool = False) -> tuple[float, float]:
    if not use_db:
        return _convert_wgs84_to_sweref99tm(lat, lon)
    info = sweref99tm_db.get(lat, lon)
    if info:
        return info["x_pos"], info["y_pos"]
    x, y = _convert_wgs84_to_sweref99tm(lat, lon)
    sweref99tm_db.add(lat, lon, x, y)
    return x, y


def get_wgs84_to_sweref99tm_mapper(
    lat: list[str], lon: list[str], use_db: bool = False
) -> (dict)[(str, str), (str, str)]:
    mapper = {}
    if use_db:
        mapper = sweref99tm_db.get_mapper()
        print(f"Getting mapper: {sweref99tm_db.DB_PATH}")
    for pos in zip(lat, lon):
        if not mapper.get(pos):
            if not pos[0] and not pos[1]:
                mapper[(pos[0], pos[1])] = ("", "")
                continue
            x, y = _convert_wgs84_to_sweref99tm(pos[0], pos[1])
            if use_db:
                sweref99tm_db.add(pos[0], pos[1], x, y)
            mapper[(pos[0], pos[1])] = (x, y)
    return mapper


def _convert_wgs84_to_sweref99tm(lat: str, lon: str) -> tuple[float, float]:
    wgs84 = pyproj.CRS("EPSG:4326")  # WGS84 i decimalgrader
    sweref99tm = pyproj.CRS("EPSG:3006")  # SWEREF 99TM
    transformer = pyproj.Transformer.from_crs(wgs84, sweref99tm, always_xy=True)
    x, y = transformer.transform(lon, lat)
    return x, y


def sweref99tm_to_wgs84(
    northing: float | str, easting: float | str
) -> tuple[float, float]:
    """
    Ex:
    northing = lat = Y: 6357045
    easitng = lon = X:  583379
    """
    northing = float(northing)
    easting = float(easting)
    sweref99tm = pyproj.CRS("EPSG:3006")  # SWEREF 99TM
    wgs84 = pyproj.CRS("EPSG:4326")  # WGS84 i decimalgrader
    transformer = pyproj.Transformer.from_crs(sweref99tm, wgs84, always_xy=True)
    # Konvertera från SWEREF 99TM till decimalgrader
    lon, lat = transformer.transform(easting, northing)
    return lat, lon


def rt90_to_wgs84(northing: float | str, easting: float | str) -> tuple[float, float]:
    """
    Ex:
    northing = lat = Y: 6358256
    easting = lon = X:  1534939
    """
    northing = float(northing)
    easting = float(easting)
    rt90_25_gon_V = pyproj.CRS("EPSG:3021")  # https://epsg.io/3021
    wgs84 = pyproj.CRS("EPSG:4326")  # WGS84 i decimalgrader
    transformer = pyproj.Transformer.from_crs(rt90_25_gon_V, wgs84, always_xy=True)
    # Konvertera från SWEREF 99TM till decimalgrader
    lon, lat = transformer.transform(easting, northing)
    return lat, lon

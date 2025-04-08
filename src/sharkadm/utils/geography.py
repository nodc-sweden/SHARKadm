import numpy as np
import pyproj


def decdeg_to_decmin(
    pos: float | str, nr_decimals: int | None = 2, with_space: bool = False
):
    pos = float(pos)
    deg = np.floor(pos)
    minute = 0
    if deg:
        minute = pos % deg * 60.0
    if nr_decimals:
        output = "%%2.%sf" % nr_decimals % (deg * 100.0 + minute)
    else:
        output = str(deg * 100.0 + minute)
    if with_space:
        a, b = output.split(".")
        output = f"{a[:-2]} {a[-2:]}.{b}"
    return output


def decmin_to_decdeg(pos: str, nr_decimals: int | None = 2):
    pos = float(pos.replace(" ", ""))
    if pos >= 0:
        output = np.floor(pos / 100.0) + (pos % 100) / 60.0
    else:
        output = np.ceil(pos / 100.0) - (-pos % 100) / 60.0

    if nr_decimals:
        output = np.round(output, nr_decimals)

    return str(output)


# def decdeg_to_sweref99tm(lat: float, lon: float) -> (float, float):
#     pp = pyproj.Proj(proj='utm', zone=33, ellps='WGS84', preserve_units=False)
#     x, y = pp(lon, lat)
#     return x, y


def decdeg_to_sweref99tm(lat: float, lon: float) -> (float, float):
    wgs84 = pyproj.CRS("EPSG:4326")  # WGS84 i decimalgrader
    sweref99tm = pyproj.CRS("EPSG:3006")  # SWEREF 99TM
    transformer = pyproj.Transformer.from_crs(wgs84, sweref99tm, always_xy=True)
    x, y = transformer.transform(lon, lat)
    return x, y


def sweref99tm_to_decdeg(x: float | str, y: float | str) -> (float, float):
    x = float(x)
    y = float(y)
    wgs84 = pyproj.CRS("EPSG:4326")  # WGS84 i decimalgrader
    sweref99tm = pyproj.CRS("EPSG:3006")  # SWEREF 99TM
    transformer = pyproj.Transformer.from_crs(sweref99tm, wgs84, always_xy=True)
    # Konvertera från SWEREF 99TM till decimalgrader
    lon, lat = transformer.transform(x, y)
    return lat, lon

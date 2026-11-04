import pyproj
from nodc_config import Config

from sharkadm.utils import sweref99tm_db


def decdeg_to_decmin(
    pos: float | str,
    nr_decimals: int | None = None,
    with_space: bool = False,
) -> str:
    """Convert decimal degrees (DD) to degrees and decimal minutes (DDM)."""

    pos = float(pos)

    sign = "-" if pos < 0 else ""
    abs_pos = abs(pos)

    if abs_pos == 0:
        return "00.0" if not with_space else "0 0.0"

    degrees = int(abs_pos)
    minutes = (abs_pos - degrees) * 60.0

    if nr_decimals is not None:
        minutes = round(minutes, nr_decimals)

        if minutes >= 60:
            degrees += 1
            minutes = 0.0

        minute_str = f"{minutes:.{nr_decimals}f}"
    else:
        minute_str = str(minutes)

    # Ensure at least two digits before decimal point
    integer_part, *decimal_part = minute_str.split(".")
    minute_str = integer_part.zfill(2)

    if decimal_part:
        minute_str += "." + decimal_part[0]

    if with_space:
        return f"{sign}{degrees} {minute_str}"

    return f"{sign}{degrees}{minute_str}"


def decmin_to_decdeg(
    pos: str | float,
    nr_decimals: int | None = None,
) -> str:
    """Convert degrees and decimal minutes (DDM) to decimal degrees (DD)."""

    pos = float(str(pos).replace(" ", ""))

    sign = -1 if pos < 0 else 1
    abs_pos = abs(pos)

    degrees = int(abs_pos // 100)
    minutes = abs_pos - degrees * 100

    if minutes >= 60:
        raise ValueError("Minutes must be less than 60")

    output = sign * (degrees + minutes / 60.0)

    if nr_decimals is not None:
        output = round(output, nr_decimals)

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
    nodc_config: Config, lat: list[str], lon: list[str], use_db: bool = False
) -> (dict)[(str, str), (str, str)]:
    db = sweref99tm_db.Sweref99tmDatabase(nodc_config)
    mapper = {}
    if use_db:
        mapper = db.get_mapper()
        print(f"Getting mapper: {db.get_db_path()}")
    for pos in zip(lat, lon):
        if not mapper.get(pos):
            if not pos[0] and not pos[1]:
                mapper[(pos[0], pos[1])] = ("", "")
                continue
            x, y = _convert_wgs84_to_sweref99tm(pos[0], pos[1])
            if use_db:
                db.add(pos[0], pos[1], x, y)
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

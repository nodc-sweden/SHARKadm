import sqlite3

import requests

from sharkadm.config import CONFIG_DIRECTORY

DB_PATH = None
if CONFIG_DIRECTORY:
    DB_PATH = CONFIG_DIRECTORY / "sharkadm" / "iobis_database.db"


def create_database():
    if DB_PATH.exists():
        return
    print(f"Creating iobis database at: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS iobis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lat_dd TEXT,
            lon_dd TEXT,
            depth TEXT,
            UNIQUE(lat_dd, lon_dd)
        );
        """

        cursor.execute(create_table_query)

        connection.commit()


def add_db_data(lat_dd: str, lon_dd: str, depth: str):
    create_database()
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO iobis (lat_dd, lon_dd, depth)
        VALUES (?, ?, ?);
        """
        data = (lat_dd, lon_dd, depth)

        cursor.execute(insert_query, data)

        connection.commit()


def get_db_data(lat_dd: str, lon_dd: str) -> dict:
    create_database()
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.cursor()

        query = """
           SELECT * FROM iobis
           WHERE lat_dd = ?
           AND lon_dd = ?
           ;
           """
        data = (lat_dd, lon_dd)

        cursor.execute(query, data)
        result = cursor.fetchone()

        connection.commit()
        info = dict()
        if result:
            info["lat_dd"] = result[1]
            info["lon_dd"] = result[2]
            info["depth"] = result[3]
        return info


def get_mapper() -> dict:
    create_database()
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.cursor()

        query = """
           SELECT * FROM iobis
           ;
           """

        cursor.execute(query)
        result = cursor.fetchall()

        connection.commit()
        info = dict()
        for res in result:
            info[(res[1], res[2])] = (res[3], res[4])
        return info


def fetch_obis_data(lat: float, lon: float) -> dict:
    """
    Hämtar data från OBIS API för koordinaterna x och y.

    Args:
        lon (float): Longitud
        lat (float): Latitud

    Returns:
        dict: JSON-svar från API:et
    """
    url = "https://api.obis.org/xylookup"
    params = {"x": lon, "y": lat}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Kastar fel om statuskod ≠ 200
        return response.json()[0]
    except requests.exceptions.RequestException:
        return {}


def get_obis_depth(lat: float, lon: float) -> float | None:
    db_data = get_db_data(str(lat), str(lon))
    if db_data:
        return db_data["depth"]
    info = fetch_obis_data(lat, lon)
    depth = float(info.get("grids", {}).get("bathymetry"))
    add_db_data(str(lat), str(lon), str(depth))
    return depth

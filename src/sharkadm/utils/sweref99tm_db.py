import sqlite3

from sharkadm.config import CONFIG_DIRECTORY

DB_PATH = CONFIG_DIRECTORY / "sharkadm" / "sweref99tm_database.db"
print(f"sweref99tm database at {DB_PATH}")

COLUMNS = [
    "id",
    "lat_dd",
    "lon_dd",
    "x_pos",
    "y_pos",
]


def create_database():
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS sweref99tm (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lat_dd TEXT,
            lon_dd TEXT,
            x_pos TEXT,
            y_pos TEXT,
            UNIQUE(lat_dd, lon_dd)
        );
        """

        cursor.execute(create_table_query)

        connection.commit()


def add(lat_dd: str, lon_dd: str, x_pos: str, y_pos: str):
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.cursor()

        insert_query = """
        INSERT INTO sweref99tm (lat_dd, lon_dd, x_pos, y_pos)
        VALUES (?, ?, ?, ?);
        """
        data = (lat_dd, lon_dd, x_pos, y_pos)

        cursor.execute(insert_query, data)

        connection.commit()


def get(lat_dd: str, lon_dd: str) -> dict:
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.cursor()

        query = """
           SELECT * FROM sweref99tm
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
            info["x_pos"] = result[3]
            info["y_pos"] = result[4]
        return info


def get_mapper() -> dict:
    with sqlite3.connect(DB_PATH) as connection:
        cursor = connection.cursor()

        query = """
           SELECT * FROM sweref99tm
           ;
           """

        cursor.execute(query)
        result = cursor.fetchall()

        connection.commit()
        info = dict()
        for res in result:
            info[(res[1], res[2])] = (res[3], res[4])
        return info


create_database()

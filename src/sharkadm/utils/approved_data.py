import pathlib

import polars as pl

from sharkadm.config import CONFIG_DIRECTORY


def add_concatenated_column(
    df: pl.DataFrame, column_name: str = "approved_key"
) -> pl.DataFrame:
    lat_col = "sample_latitude_dd"
    lon_col = "sample_longitude_dd"
    if "sample_latitude_dd_float" in df.columns:
        lat_col = "sample_latitude_dd_float"
    if "sample_longitude_dd_float" in df.columns:
        lon_col = "sample_longitude_dd_float"

    return df.with_columns(
        pl.concat_str(
            [
                # pl.col("delivery_datatype"),
                pl.col(lat_col).cast(str),
                pl.col(lon_col).cast(str),
                # pl.col("dataset_name"),
                # pl.col("sample_date").cast(str).str.replace_all("-", ""),
                # pl.col("reported_station_name"),
            ],
            separator=":",
        ).alias(column_name)
    )


class ApprovedData:
    def __init__(self):
        self._df: pl.DataFrame = pl.DataFrame()
        self._mapper: dict[str, bool] = dict()
        self._paths: list[pathlib.Path] = []

        self._load_data()
        self._add_concatenated_column()
        self._create_mapper()

    def __repr__(self):
        file_names = "; ".join([p.name for p in self._paths])
        return f"Approved data: {file_names}"

    @property
    def mapper(self) -> dict[str, bool]:
        return self._mapper

    @property
    def df(self) -> pl.DataFrame:
        return self._df

    def _load_data(self) -> None:
        dfs = []
        directory = CONFIG_DIRECTORY / "sharkadm" / "approved_data"
        for path in directory.iterdir():
            if path.suffix != ".csv":
                continue
            self._paths.append(path)
            dfs.append(pl.read_csv(path))
        self._df = pl.concat(dfs)

    def _add_concatenated_column(self) -> None:
        self._df = add_concatenated_column(self._df)

    def _create_mapper(self) -> None:
        records = self._df.to_dicts()
        self._mapper = {rec["approved_key"]: True for rec in records}


if __name__ == "__main__":
    ap = ApprovedData()

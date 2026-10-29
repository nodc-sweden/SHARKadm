from typing import Any

import polars as pl

from sharkadm.data.data_holder import PolarsDataHolder


def get_data_holder_statistics(data_holder: PolarsDataHolder) -> dict[str, Any]:
    info = dict()
    if "datetime" in data_holder.columns:
        info["start_datetime"] = min(data_holder.data["datetime"])
        info["end_datetime"] = max(data_holder.data["datetime"])

    if "visit_key" in data_holder.columns:
        info["nr_visits"] = len(set(data_holder.data["visit_key"]))
    else:
        info["nr_visits"] = len(
            list(
                data_holder.data.group_by(
                    ["reported_latitude", "reported_longitude", "datetime"]
                )
            )
        )

    info["nr_unique_station_names"] = len(set(data_holder.data["reported_station_name"]))
    info["nr_unique_positions"] = len(
        list(data_holder.data.group_by(["reported_latitude", "reported_longitude"]))
    )
    info["nr_unique_datetimes"] = len(list(data_holder.data.group_by(["datetime"])))

    info["nr_rows"] = len(data_holder.data)
    info["data_structure"] = f"{data_holder.data_structure}-format"

    return info


def get_nr_positions_per_reported_station_name(
    data: pl.DataFrame, min_nr: int | None, mapped_station: bool | None = None
) -> pl.DataFrame:
    station_col = "reported_station_name"
    if mapped_station:
        station_col = "station_name"

    data = (
        data.with_columns(
            pl.concat_str(
                ["reported_latitude", "reported_longitude"], separator="_"
            ).alias("pos")
        )
        .group_by(station_col)
        .n_unique()
        .with_columns(pl.col("pos").alias("nr_unique_positions"))[
            station_col, "nr_unique_positions"
        ]
    )
    if min_nr is not None:
        data = data.filter(pl.col("nr_unique_positions") >= min_nr)
    return data.sort(station_col)

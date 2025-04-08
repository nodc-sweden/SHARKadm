from typing import Any

from sharkadm.data.data_holder import PandasDataHolder


def get_data_holder_statistics(data_holder: PandasDataHolder) -> dict[str, Any]:
    info = dict()
    if "datetime" in data_holder.columns:
        info["start_datetime"] = min(data_holder.data["datetime"])
        info["end_datetime"] = max(data_holder.data["datetime"])

    if "visit_key" in data_holder.columns:
        info["nr_visits"] = len(data_holder.data.groupby("visit_key"))

    info["nr_stations"] = len(set(data_holder.data["reported_station_name"]))
    info["nr_rows"] = len(data_holder.data)
    info["data_structure"] = f"{data_holder.data_structure}-format"
    return info

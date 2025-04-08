import datetime
import json
import pathlib

import pandas as pd


def get_meta_info():
    return dict(
        version="0.0.1", time=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    )


def get_meta_compose_info():
    return dict(compose=get_meta_info())


def save_data_to_file(data: dict, path: pathlib.Path):
    with open(path, "w") as fid:
        json.dump(data, fid, indent=2)


class IfcbVisualizationAnalyzesFiles:
    def __init__(self, data: pd.DataFrame, directory: str | pathlib.Path, **kwargs):
        self._data = data
        self._directory = pathlib.Path(directory)

        self._create_analyzes_files()

    def _create_analyzes_files(self):
        # One file per day
        for date_str, day_df in self._data.groupby("visit_date"):
            data = dict(analyzes=[], meta=get_meta_info())
            for dtime, dtime_df in day_df.groupby("datetime"):
                sample_time = dtime.strftime("%Y-%m-%d %H:%M:%S")
                sample_data = dict()
                sample_data["sample"] = dict(
                    sampleTime=sample_time,
                    sampleCoordinates=[
                        float(dtime_df["sample_latitude_dd"].values[0]),
                        float(dtime_df["sample_longitude_dd"].values[0]),
                    ],
                )

                sample_data["basin"] = dict(
                    id=None,
                    name=None,
                )

                sample_data["analysis"] = dict(
                    sampleTime=sample_time,
                    countByTaxon=self._get_count_by_taxon(dtime_df),
                )
                data["analyzes"].append(sample_data)

            file_path = self._directory / f"analyzes-{date_str}.json"
            with open(file_path, "w") as fid:
                json.dump(data, fid, indent=2)

    @staticmethod
    def _get_count_by_taxon(df: pd.DataFrame) -> dict[str, int]:
        counts = {}
        count_df = df.loc[df["parameter"] == "# counted"]
        for (name, count), _df in count_df.groupby(["scientific_name", "value"]):
            counts[name] = int(count)
        return counts


class IfcbVisualizationCruises:
    def __init__(
        self,
        data: pd.DataFrame,
        directory: str | pathlib.Path,
        append: bool = True,
        **kwargs,
    ):
        self._data = data
        self._directory = pathlib.Path(directory)
        self._append = append

        self._cruise_file_path = self._directory / "cruises.json"

        self._create_cruises_file()

    def _create_cruises_file(self):
        data = dict(cruises=[], meta=get_meta_compose_info())

        for col in ["CRUISE_NO", "expedition_id", "cruise_start_serno"]:
            if col in self._data:
                cruise_col = col
                break
        else:
            raise KeyError("No column found for cruise number")

        for cruise, df in self._data.groupby(cruise_col):
            cruise_data = dict(
                startTime=min(df["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                stopTime=max(df["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
            )
            route_list = []
            for (lat, lon), dtime_df in df.sort_values("datetime").groupby(
                ["sample_latitude_dd", "sample_longitude_dd"]
            ):
                route_list.append([float(lat), float(lon)])
            cruise_data["route"] = route_list
            data["cruises"].append(cruise_data)

        if self._cruise_file_path.exists() and self._append:
            self._append_cruise_file(data)
        else:
            save_data_to_file(data, self._cruise_file_path)

    def _append_cruise_file(self, data: dict) -> None:
        if not self._cruise_file_path.exists():
            return
        with open(self._cruise_file_path) as fid:
            old_data = json.load(fid)
        old_cruise_mapper = {
            f"{item['startTime']}_{item['stopTime']}": item
            for item in old_data["cruises"]
        }
        new_cruise_mapper = {
            f"{item['startTime']}_{item['stopTime']}": item for item in data["cruises"]
        }

        all_cruises = []
        for new_key, new_cruise in new_cruise_mapper.items():
            if old_cruise_mapper.get(new_key):
                old_cruise_mapper.pop(new_key)
            all_cruises.append(new_cruise)
        all_cruises.extend(list(old_cruise_mapper.values()))

        all_cruises = sorted(all_cruises, key=lambda x: x["startTime"])

        data = dict(cruises=all_cruises, meta=get_meta_compose_info())
        save_data_to_file(data, self._cruise_file_path)


def create_ifcb_visualization_files(
    data: pd.DataFrame, directory: str | pathlib.Path, append: bool = True
):
    IfcbVisualizationAnalyzesFiles(data, directory)
    IfcbVisualizationCruises(data, directory, append=append)

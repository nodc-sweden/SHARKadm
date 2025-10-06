import datetime
import logging

import polars as pl

from sharkadm import utils
from sharkadm.data.data_source.base import DataFile, PolarsDataFile
from sharkadm.data.data_source.profile.mapper import get_ctd_parameter_mapper
from sharkadm.data.archive import metadata

mapper = get_ctd_parameter_mapper()


class CnvDataFile(PolarsDataFile):

    def __init__(self, *args, **kwargs):
        self._metadata_lines: list[str] = []
        super().__init__(*args, **kwargs)

    def __getitem__(self, item: str) -> list[str] | str:
        values = set(self._data[item])
        if len(values) == 1:
            return values.pop()
        return sorted(values)

    def _load_file(self) -> None:
        header = []
        data_lines = []
        metadata = {}
        is_data_line = False
        with open(self._path, encoding="cp1252") as fid:
            for r, line in enumerate(fid):
                if not line.strip():
                    continue
                if not is_data_line:
                    self._metadata_lines.append(line)
                if line.startswith("* NMEA Latitude"):
                    metadata["LATIT"] = "".join([c for c in line.split("=", 1)[-1] if
                                                 c.isdigit() or c == "."])
                elif line.startswith("* NMEA Longitude"):
                    metadata["LONGI"] = "".join([c for c in line.split("=", 1)[-1] if
                                                 c.isdigit() or c == "."]).lstrip("0")
                elif line.startswith("* System UpLoad Time"):
                    dtime = datetime.datetime.strptime(line.split("=", 1)[-1].strip(),
                                                       "%b %d %Y %H:%M:%S")
                    metadata["SDATE"] = dtime.strftime("%Y-%m-%d")
                    metadata["STIME"] = dtime.strftime("%H:%M")
                elif line.startswith("** Station:"):
                    metadata["STATN"] = line.split(":", 1)[-1].strip()
                if line.startswith("# name"):
                    name = line.split("=", 1)[-1].strip()
                    header.append(mapper.get(name, name))
                elif line.startswith("*END*"):
                    is_data_line = True
                    continue
                elif is_data_line:
                    data_lines.append(dict(zip(header, line.strip().split())))

        self._data = pl.DataFrame(data_lines)

        # Add metadata
        for col, val in metadata.items():
            self._data = self._data.with_columns(
                pl.lit(val).alias(col)
            )

    def add_metadata(self, data: metadata.Metadata) -> None:
        kw = {
            "SDATE": self.data[0, "SDATE"],
            "STIME": self.data[0, "STIME"]
        }
        meta = data.get_info(**kw)
        if len(meta) != 1:
            raise Exception("Metadata error")
        for col, value in meta[0].items():
            self._data = self._data.with_columns(
                pl.lit(value).alias(col)
            )


if __name__ == "__main__":

    csv = CnvDataFile(r"C:\mw\data\input_sharkadm\new_profile\SHARK_Profile_2023_BAS_SMHI_version_2025-09-29\received_data\SBE09_1044_20230110_1036_77SE_01_0001.cnv")
    meta = metadata.Metadata.from_txt_file(r"C:\mw\data\input_sharkadm\new_profile\SHARK_Profile_2023_BAS_SMHI_version_2025-09-29\received_data\metadata.txt")

    csv.add_metadata(meta)
    # import json
    # import yaml
    #
    # with open(r"C:\mw\git\nodc_config\sharkadm\ctd_parameter_mapping.json", encoding="utf8") as fid:
    #     data = json.load(fid)
    # new_data = {}
    # for key, values in data["mapping_parameter"].items():
    #     for value in values:
    #         new_data[value] = key
    # with open(r"C:\mw\git\nodc_config\sharkadm\ctd_parameter_mapping.yaml", "w", encoding="utf8") as fid:
    #     yaml.safe_dump(new_data, fid)

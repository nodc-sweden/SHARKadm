import datetime

import polars as pl

from sharkadm.data.data_source.base import PolarsDataFile
from sharkadm.data.data_source.profile.mapper import get_ctd_parameter_mapper


class CnvDataFile(PolarsDataFile):
    def __init__(self, *args, **kwargs):
        self._metadata_lines: list[str] = []
        super().__init__(*args, **kwargs)
        self._mapper = get_ctd_parameter_mapper()

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
                    metadata["LATIT"] = "".join(
                        [c for c in line.split("=", 1)[-1] if c.isdigit() or c == "."]
                    )
                elif line.startswith("* NMEA Longitude"):
                    metadata["LONGI"] = "".join(
                        [c for c in line.split("=", 1)[-1] if c.isdigit() or c == "."]
                    ).lstrip("0")
                elif line.startswith("* System UpLoad Time"):
                    dtime = datetime.datetime.strptime(
                        line.split("=", 1)[-1].strip(), "%b %d %Y %H:%M:%S"
                    )
                    metadata["SDATE"] = dtime.strftime("%Y-%m-%d")
                    metadata["STIME"] = dtime.strftime("%H:%M:%S")
                elif line.startswith("** Station:"):
                    metadata["STATN"] = line.split(":", 1)[-1].strip()
                if line.startswith("# name"):
                    name = line.split("=", 1)[-1].strip()
                    if ", lat =" in name:
                        name = name.split(", lat =")[0]
                    header.append(self._mapper.get(name, name))
                elif line.startswith("*END*"):
                    is_data_line = True
                    continue
                elif is_data_line:
                    data_lines.append(dict(zip(header, line.strip().split())))

        self._data = pl.DataFrame(data_lines)

        # Add metadata
        for col, val in metadata.items():
            self._data = self._data.with_columns(pl.lit(val).alias(col))

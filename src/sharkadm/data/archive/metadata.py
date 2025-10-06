# -*- coding: utf-8 -*-

import datetime
import logging
import pathlib
from typing import Protocol
import polars as pl

from sharkadm.sharkadm_logger import adm_logger

logger = logging.getLogger(__name__)

DATE_FORMATS = ["%Y-%m-%d", "%Y-%m"]


class Mapper(Protocol):
    def mapper(self, external_par: str) -> str: ...


class Metadata:
    def __init__(self, data: pl.DataFrame,
                 mapper: Mapper = None,
                 path: pathlib.Path = None) -> None:
        self._data = data
        self._mapper = mapper
        self._path = path

        if self._mapper:
            self._map_data()

    def _map_data(self):
        if not self._mapper:
            adm_logger.log_workflow(
                f"No mapper found when trying to map Metadata file: {self._path}"
            )
            return
        # new_cols = {col: self._mapper.get_internal_name(col) for col in self._data.columns}
        print(f"{self._mapper.mapper=}")
        self._data = self._data.rename(self._mapper.mapper, strict=False)

    def __str__(self):
        return f"Metadata file: {self._path}"

    def get_info(self, **kwargs) -> list[dict]:
        boolean = pl.Series([True] * len(self._data))
        for key, value in kwargs.items():
            boolean = boolean & (pl.col(key) == value)
        records = self._data.filter(boolean).to_dicts()
        return records

    @classmethod
    def from_txt_file(
        cls, path: str | pathlib.Path, mapper: Mapper = None, encoding: str = "cp1252"
    ) -> "Metadata":
        data = pl.read_csv(
            path,
            encoding=encoding,
            separator="\t",
            infer_schema=False,
            missing_utf8_is_empty_string=True,
            truncate_ragged_lines=True
        )
        return Metadata(data, mapper=mapper, path=path)

    # @classmethod
    # def from_dv_template(
    #     cls, path: str | pathlib.Path, mapper: Mapper = None
    # ) -> "Metadata":
    #     wb = openpyxl.load_workbook(path)
    #     sheet_name = None
    #     for name in ["Provtagningsinfo"]:
    #         if name in wb.sheetnames:
    #             sheet_name = name
    #             break
    #     if not sheet_name:
    #         adm_logger.log_workflow(
    #             f"Could not find sampling_info sheet in file: {path}",
    #             level=adm_logger.WARNING,
    #         )
    #         return
    #         # raise Exception(f'Could not find analyse_info sheet in file: {path}')
    #
    #     ws = wb.get_sheet_by_name(sheet_name)
    #     skip_nr_rows = 0
    #     for r in range(1, 5):
    #         if ws.cell(r, 1).value in ["Tabellhuvud:"]:
    #             skip_nr_rows = r - 1
    #             break
    #     df = pd.read_excel(path, skiprows=skip_nr_rows, sheet_name=sheet_name, dtype=str)
    #     df.fillna("", inplace=True)
    #     data = dict()
    #     data["path"] = path
    #     for row in df.iterrows():
    #         line_dict = row[1].to_dict()
    #         if not line_dict["PARAM"]:
    #             continue
    #         line_dict["VALIDFR"] = _get_date(line_dict["VALIDFR"])
    #         line_dict["VALIDTO"] = _get_date(line_dict["VALIDTO"])
    #         par = line_dict["PARAM"]
    #         data.setdefault(par, [])
    #         data[par].append(line_dict)
    #     return Metadata(data, mapper=mapper)

    @property
    def data(self) -> dict[str, str]:
        return self._data

    @property
    def parameters(self) -> list[str]:
        """Returns a list of all parameters in the file. The list is unsorted."""
        return list(self._data)

    @property
    def columns(self) -> list[str]:
        return list(next(iter(self.data[next(iter(self._data))])))


def _get_date(date_str: str) -> datetime.date | str:
    if not date_str:
        return ""
    d_string = date_str.split()[0]
    for form in DATE_FORMATS:
        try:
            return datetime.datetime.strptime(d_string, form).date()
        except ValueError:
            pass
    adm_logger.log_workflow(
        "Invalid date or date format in sampling_info",
        item=date_str,
        level=adm_logger.ERROR,
    )
    adm_logger.log_workflow(
        adm_logger.feedback.invalid_date_in_analys_info(date_str),
        level=adm_logger.ERROR,
        purpose=adm_logger.FEEDBACK,
    )
    return ""

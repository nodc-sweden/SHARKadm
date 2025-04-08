import datetime
import logging
import shutil
from typing import Any, Protocol, Type

import pandas as pd
import requests

from sharkadm import utils
from sharkadm.data.data_holder import PandasDataHolder
from sharkadm.data.data_source.base import DataFile
from sharkadm.data.data_source.txt_file import TxtRowFormatDataFile

logger = logging.getLogger(__name__)


class HeaderMapper(Protocol):
    def get_internal_name(self, external_par: str) -> str: ...


class SHARKapiDataHolder(PandasDataHolder):
    _data_type = ""

    def __init__(
        self,
        data_type: str,
        year: int | None = None,
        header_mapper: HeaderMapper = None,
        encoding: str = "utf8",
        **kwargs,
    ):
        super().__init__()

        self._data_type = data_type
        self._encoding = encoding

        self.query = {
            "bounds": [],
            "fromYear": None,
            "toYear": None,
            "months": [],
            "dataTypes": [],
            "parameters": [],
            "checkStatus": "",
            "qualityFlags": [],
            "deliverers": [],
            "orderers": [],
            "projects": [],
            "datasets": [],
            "minSamplingDepth": "",
            "maxSamplingDepth": "",
            "redListedCategory": [],
            "taxonName": [],
            "stationName": [],
            "vattenDistrikt": [],
            "seaBasins": [],
            "counties": [],
            "municipalities": [],
            "waterCategories": [],
            "typOmraden": [],
            "helcomOspar": [],
            "seaAreas": [],
        }

        self.query.update(kwargs)
        self.query["dataTypes"] = [data_type]

        if year:
            self._query["fromYear"] = int(year)
            self._query["toYear"] = int(year)

        self._header_mapper = header_mapper

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._temp_directory = utils.get_temp_directory(
            "shark_api_data", datetime.datetime.now().strftime("%Y%M%d_%H%M%S")
        )

    def get_query_options(self) -> Type[list[Any]]:
        return list[self.query]

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data accessed through shark_api api"""

    @property
    def _temp_file_path(self):
        return self._temp_directory / f"{self.dataset_name}.txt"

    def _clear_temp_directory(self):
        shutil.rmtree(self._temp_directory)

    def _load_data(self):
        self._clear_temp_directory()
        all_data = self._get_data_from_api()
        self._data = pd.DataFrame(all_data["rows"], columns=all_data["headers"])

    def _get_data_from_api(self) -> dict:
        url = "https://shark.smhi.se/api/sample/table"
        headers = {"accept": "application/json", "Content-Type": "application/json"}

        base_data = {
            "params": {
                "tableView": "sharkweb_overview",
                "limit": 200,
                "offset": 0,
                "headerLang": "sv",
            },
            "query": self._query,
        }

        all_data = dict(headers=[], rows=[])
        limit = base_data["params"]["limit"]
        offset = 0

        while True:
            base_data["params"]["offset"] = offset

            response = requests.post(url, headers=headers, json=base_data)

            if response.status_code != 200:
                raise ResourceWarning("SHARK url request error")
            data = response.json()
            if not data["rows"]:
                break
            all_data["headers"] = data["headers"]
            all_data["rows"].extend(data["rows"])

            offset += limit

        return all_data

    def _load_file(self) -> None:
        d_source = TxtRowFormatDataFile(
            path=self._temp_file_path, data_type=self.data_type
        )
        if self._header_mapper:
            d_source.map_header(self._header_mapper)
        self._data = self._get_data_from_data_source(d_source)

    @staticmethod
    def _get_data_from_data_source(data_source: DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna("")
        data.reset_index(inplace=True, drop=True)
        return data

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def dataset_name(self) -> str:
        return f"SHARKweb_data_{self._data_type}_{self._from_year}-{self._to_year}"

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)

import datetime
import json
import logging
import shutil
import uuid
from typing import Protocol

import pandas as pd
import requests

from SHARKadm import utils
from SHARKadm.data import data_source
from SHARKadm.data.data_holder import DataHolder

logger = logging.getLogger(__name__)


class HeaderMapper(Protocol):

    def get_internal_name(self, external_par: str) -> str:
        ...


class SHARKwebDataHolder(DataHolder):
    _data_type = ''

    def __init__(self,
                 data_type: str,
                 year: int = None,
                 from_year: int = datetime.datetime.now().year,
                 to_year: int = datetime.datetime.now().year,
                 header_mapper: HeaderMapper = None,
                 **kwargs):

        self._data_type = data_type
        if year:
            self._from_year = self._to_year = year
        else:
            self._from_year = from_year
            self._to_year = max(from_year, to_year)

        self._encoding = kwargs.get('encoding', 'utf8')

        self._header_mapper = header_mapper

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._temp_directory = utils.get_temp_directory('sharkweb_data', datetime.datetime.now().strftime('%Y%M%d_%H%M%S'))

        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data accessed through sharkweb api"""

    @property
    def _temp_file_path(self):
        return self._temp_directory / f'{self.dataset_name}.txt'

    def _clear_temp_directory(self):
        shutil.rmtree(self._temp_directory)

    def _load_data(self):
        self._clear_temp_directory()
        # self._download_data()
        self._load_file()

    def _download_data(self):

        # We need to split download into several ones due to timeouts
        # on server. It seems that we can query a full year without a
        # timeout appear, so lets do that.

        # if self.no_download and os.path.exists(filename):
        #     self.file_explorer_logger.info('Skipping download, file exists \'%s\'' % filename)
        #     continue

        payload = {

            'params': {
                'headerLang': 'short',
                'encoding': self._encoding,
                'delimiters': 'point-tab',
                'tableView': 'sample_col_physicalchemical_columnparams'
            },

            'query': {
                'fromYear': self._from_year,
                'toYear': self._to_year,
                'dataTypes': self._data_type,
                # 'projects': ["NAT Nationell miljöövervakning"],
                # 'bounds': [[10.4, 58.2], [10.6, 58.3]],
            },

            'downloadId': str(uuid.uuid4()),
        }

        api_server = 'https://sharkweb.smhi.se'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'text/plain',
        }

        with requests.post('%s/api/sample/download' % api_server,
                           data=json.dumps(payload), headers=headers) as response:

            response.raise_for_status()

            data_location = response.headers['location']
            with requests.get('%s%s' % (api_server, data_location), stream=True) as data_response:
                data_response.raise_for_status()
                chunk_size = 1024 * 1024
                with open(self._temp_file_path, 'wb') as data_file:
                    for chunk in data_response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        data_file.write(chunk)

    def _load_file(self) -> None:
        d_source = data_source.TxtRowFormatDataFile(path=self._temp_file_path, data_type=self.data_type)
        if self._header_mapper:
            d_source.map_header(self._header_mapper)
        self._data = self._get_data_from_data_source(d_source)

    @staticmethod
    def _get_data_from_data_source(data_source: data_source.DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna('')
        data.reset_index(inplace=True, drop=True)
        return data

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def dataset_name(self) -> str:
        return f'SHARKweb_data_{self._data_type}_{self._from_year}-{self._to_year}'

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)






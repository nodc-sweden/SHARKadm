# -*- coding: utf-8 -*-

import datetime
import pathlib
import pandas as pd
import logging

from SHARKadm import config

logger = logging.getLogger(__name__)


class DeliveryNote:

    # def __init__(self, path: str | pathlib.Path, encoding: str = 'cp1252') -> None:
    def __init__(self, data: dict) -> None:
        self._data = data
        self._path = data.pop('path', None)
        self._data_format = data.pop('data_format', None)
        self._import_matrix_key = data.pop('import_matrix_key', None)

    def __getitem__(self, item: str) -> str:
        return self._data[item]

    @classmethod
    def from_txt_file(cls, path: str | pathlib.Path, encoding: str = 'cp1252') -> 'DeliveryNote':
        path = pathlib.Path(path)
        if path.suffix != '.txt':
            msg = f'Fil is not a valid xlsx dv template: {path}'
            logger.error(msg)
            raise FileNotFoundError(msg)
        data = dict()
        data['path'] = path
        with open(path, encoding=encoding) as fid:
            for line in fid:
                if not line.strip():
                    continue
                if ':' not in line:
                    # Belongs to previous row
                    data[key] = f'{data[key]} {line.strip()}'
                    continue
                key, value = [item.strip() for item in line.split(':', 1)]
                data[key] = value
                if key == 'format':
                    parts = [item.strip() for item in value.split(':')]
                    data['data_format'] = parts[0]
                    data['import_matrix_key'] = parts[1]
        return DeliveryNote(data)

    @classmethod
    def from_dv_template(cls, path: str | pathlib.Path):
        path = pathlib.Path(path)
        if path.suffix != '.xlsx':
            msg = f'Fil is not a valid xlsx dv template: {path}'
            logger.error(msg)
            raise FileNotFoundError(msg)

        mapper = config.get_delivery_note_mapper()

        dn = pd.read_excel(path, sheet_name='Förklaring')
        dn['key_row'] = dn[dn.columns[0]].apply(lambda x: True if type(x) == str and x.isupper() else False)

        fdn = dn[dn['key_row']]

        col_mapping = dict((c, col) for c, col in enumerate(dn.columns))

        data = dict()
        data['path'] = path
        for key, value in zip(fdn[col_mapping[0]], fdn[col_mapping[2]]):
            if str(value) == 'nan':
                value = ''
            elif type(value) == datetime.datetime:
                value = value.date()
            data[mapper.get_txt_key_from_xlsx_key(key)] = str(value)
        data['data_format'] = data['format']
        data['import_matrix_key'] = data['format']
        if data['format'] == 'PP':
            data['data_format'] = 'Phytoplankton'
            data['import_matrix_key'] = data['PP_REG']

        return DeliveryNote(data)
    # def _load_file(self) -> None:
    #     with open(self._path, encoding=self._encoding) as fid:
    #         for line in fid:
    #             if not line.strip():
    #                 continue
    #             if ':' not in line:
    #                 # Belongs to previous row
    #                 self._data[key] = f'{self._data[key]} {line.strip()}'
    #                 continue
    #             key, value = [item.strip() for item in line.split(':', 1)]
    #             self._data[key] = value
    #             if key == 'format':
    #                 parts = [item.strip() for item in value.split(':')]
    #                 self._data_format = parts[0]
    #                 self._import_matrix_key = parts[1]

    @property
    def data_type(self) -> str:
        return self._data['datatyp'].lower()

    @property
    def data_format(self) -> str:
        return self._data_format.lower()

    @property
    def import_matrix_key(self) -> str:
        """This it the key that is used in the import matrix to find the correct parameter mapping"""
        return self._import_matrix_key

    @property
    def fields(self) -> list[str]:
        """Returns a list of all the fields in teh file. The list is unsorted."""
        return list(self._data)

    def __getitem__(self, item: str) -> str:
        """Returns the corresponding value for the given field"""
        return self._data[item]

    @property
    def status(self) -> str:
        return self['status']

    @property
    def date_reported(self):
        return datetime.datetime.strptime(self['rapporteringsdatum'], '%Y-%m-%d')

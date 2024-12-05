# -*- coding: utf-8 -*-

import datetime
import pathlib
import pandas as pd
import logging

from sharkadm import config
from sharkadm import adm_logger
from sharkadm import sharkadm_exceptions
from typing import Protocol

try:
    import nodc_codes
except ImportError:
    pass

logger = logging.getLogger(__name__)


class Mapper(Protocol):

    def get_internal_name(self, external_par: str) -> str:
        ...


class DeliveryNote:

    def __init__(self, data: dict, mapper: Mapper = None) -> None:
        self.translate_codes = nodc_codes.get_translate_codes_object()
        self._data = {key.upper(): value for key, value in data.items()}
        self._path = data.pop('path', None)
        self._data_format = data.get('data_format', None)
        self._import_matrix_key = data.get('import_matrix_key', None)
        self._mapper = mapper

        dtype = self.translate_codes.get_translation(field='delivery_datatype', synonym=self._data['DTYPE'], translate_to='internal_value')

        if not dtype:
            raise sharkadm_exceptions.DeliveryNoteError(f'Missing translation for datatype {self._data["DTYPE"]} in file {self.translate_codes.path}', level=adm_logger.ERROR)

        self._data['DTYPE'] = dtype

        #TODO: Fullhack
        if self._data['DTYPE'] == 'Physical and Chemical':
            self._data['DTYPE'] = 'PhysicalChemical'

        if self._mapper:
            self._map_data()

    def _map_data(self):
        new_data = {}
        for par, value in self._data.items():
            internal_par = self._mapper.get_internal_name(par)
            new_data[internal_par] = value
        self._data = new_data

    def __str__(self):
        lst = []
        for key, value in self._data.items():
            lst.append(f'{key}: {value}')
        return '\n'.join(lst)

    def __getitem__(self, item: str) -> str:
        return self._data.get(item)

    @classmethod
    def from_txt_file(cls, path: str | pathlib.Path, mapper: Mapper = None, encoding: str = 'cp1252') -> 'DeliveryNote':
        path = pathlib.Path(path)
        if path.suffix != '.txt':
            msg = f'File is not a valid delivery_note text file: {path}'
            logger.error(msg)
            raise FileNotFoundError(msg)

        dn_mapper = config.get_delivery_note_mapper()

        data = dict()
        data['path'] = path
        with open(path, encoding=encoding) as fid:
            for line in fid:
                if not line.strip():
                    continue
                if ':' not in line:
                    # Belongs to previous row
                    data[mapped_key] = f'{data[mapped_key]} {line.strip()}'
                    continue
                key, value = [item.strip() for item in line.split(':', 1)]
                key = key.lstrip('- ')
                mapped_key = dn_mapper.get(key)
                data[mapped_key] = value
                if key.upper() == 'FORMAT':
                    parts = [item.strip() for item in value.split(':')]
                    data['data_format'] = parts[0]
                    if len(parts) == 1:
                        msg = f'Can not find any import_matrix_key (data_format) in delivery_note: {path}'
                        raise sharkadm_exceptions.NoDataFormatFoundError(msg)
                    data['import_matrix_key'] = parts[1]
        return DeliveryNote(data, mapper=mapper)

    @classmethod
    def from_dv_template(cls, path: str | pathlib.Path, mapper: Mapper = None):
        path = pathlib.Path(path)
        if path.suffix != '.xlsx':
            msg = f'File is not a valid xlsx dv template: {path}'
            logger.error(msg)
            raise FileNotFoundError(msg)

        dn_mapper = config.get_delivery_note_mapper()

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
            data[dn_mapper.get(key)] = str(value)
        data['data_format'] = data['FORMAT']
        data['import_matrix_key'] = data['FORMAT']
        if data['FORMAT'] == 'PP':
            data['data_format'] = 'Phytoplankton'
            data['DTYPE'] = 'Phytoplankton'
            data['import_matrix_key'] = 'PP_REG'
        return DeliveryNote(data, mapper=mapper)

    @property
    def data(self) -> dict[str, str]:
        return self._data

    @property
    def data_type(self) -> str:
        return self._data['DTYPE'].lower()

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

    @property
    def status(self) -> str:
        return self['STATUS']

    @property
    def reporting_institute_code(self):
        return self['reporting_institute_code'].upper()

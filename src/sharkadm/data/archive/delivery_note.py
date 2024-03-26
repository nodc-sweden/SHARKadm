# -*- coding: utf-8 -*-

import datetime
import pathlib
import pandas as pd
import logging

from sharkadm import config
from sharkadm import adm_logger
from sharkadm import sharkadm_exceptions

import nodc_codes

logger = logging.getLogger(__name__)


class DeliveryNote:
    translate_codes = nodc_codes.get_translate_codes_object()

    # def __init__(self, path: str | pathlib.Path, encoding: str = 'cp1252') -> None:
    def __init__(self, data: dict) -> None:
        self._data = data
        self._path = data.pop('path', None)
        self._data_format = data.get('data_format', None)
        self._import_matrix_key = data.get('import_matrix_key', None)

        self._data['datatyp'] = self.translate_codes.get_translation(field='delivery_datatype', synonym=self._data['datatyp'], translate_to='public_value')

        #TODO: Fullhack
        if self._data['datatyp'] == 'Physical and Chemical':
            self._data['datatyp'] = 'PhysicalChemical'

    def __str__(self):
        lst = []
        for key, value in self._data.items():
            lst.append(f'{key}: {value}')
        return '\n'.join(lst)

    def __getitem__(self, item: str) -> str:
        return self._data.get(item)

    @classmethod
    def from_txt_file(cls, path: str | pathlib.Path, encoding: str = 'cp1252') -> 'DeliveryNote':
        path = pathlib.Path(path)
        if path.suffix != '.txt':
            msg = f'File is not a valid delivery_note text file: {path}'
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
                key = key.lstrip('- ')
                data[key] = value
                if key == 'format':
                    parts = [item.strip() for item in value.split(':')]
                    data['data_format'] = parts[0]
                    if len(parts) == 1:
                        msg = f'Can not find any import_matrix_key (data_format) in delivery_note: {path}'
                        raise sharkadm_exceptions.NoDataFormatFoundError(msg)
                    data['import_matrix_key'] = parts[1]
                    # print(data['import_matrix_key'])
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
            print(f'{key=}  :  {value=}')
            if str(value) == 'nan':
                value = ''
            elif type(value) == datetime.datetime:
                value = value.date()
            data[mapper.get_txt_key_from_xlsx_key(key)] = str(value)
        data['data_format'] = data['format']
        data['import_matrix_key'] = data['format']
        if data['format'] == 'PP':
            data['data_format'] = 'Phytoplankton'
            data['datatyp'] = 'Phytoplankton'
            data['import_matrix_key'] = 'PP_REG'
        return DeliveryNote(data)

    @property
    def data(self) -> dict[str, str]:
        return self._data

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

    @property
    def status(self) -> str:
        return self['status']

    @property
    def date_reported(self):
        return datetime.datetime.strptime(self['rapporteringsdatum'], '%Y-%m-%d')

    @property
    def reporting_institute(self):
        return self['rapporterande institut']

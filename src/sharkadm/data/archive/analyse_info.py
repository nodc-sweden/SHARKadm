# -*- coding: utf-8 -*-

import datetime
import logging
import pathlib
from typing import Protocol

from sharkadm import adm_logger

logger = logging.getLogger(__name__)


class Mapper(Protocol):

    def get_internal_name(self, external_par: str) -> str:
        ...


class AnalyseInfo:

    def __init__(self, data: dict, mapper: Mapper) -> None:
        self._data = data
        self._path = data.pop('path', None)
        self._mapper = mapper

        self._map_data()

    def _map_data(self):
        new_data = {}
        for par, info_list in self._data.items():
            internal_par = self._mapper.get_internal_name(par)
            if internal_par.startswith('COPY_VARIABLE'):
                internal_par = internal_par.split('.')[1]
            new_data.setdefault(internal_par, [])
            for info in info_list:
                new_info = dict()
                for key, value in info.items():
                    internal_key = self._mapper.get_internal_name(key)
                    new_info[internal_key] = value
                new_data[internal_par].append(new_info)
        self._data = new_data

    def __str__(self):
        lst = '\n'.join([sorted(self.data)])
        return f'Analyse info for parameters: \n{lst}'

    def get_info(self, par: str, date: datetime.date) -> dict:
        info = {}
        for info in self.data.get(par, []):
            if info['VALIDFR'] and info['VALIDTO'] and (info['VALIDFR'] <= date <= info['VALIDTO']):
                return info
            if info['VALIDFR'] and (info['VALIDFR'] <= date):
                return info
            if info['VALIDTO'] and (date <= info['VALIDTO']):
                return info
        return info

    def get(self, par: str, col: str, date: datetime.date) -> str | None:
        info = self.get_info(par=par, date=date)
        return info.get(col)

    @classmethod
    def from_txt_file(cls, path: str | pathlib.Path, mapper: Mapper, encoding: str = 'cp1252') -> 'AnalyseInfo':
        path = pathlib.Path(path)
        if path.suffix != '.txt':
            msg = f'File is not a valid analyse_info text file: {path}'
            logger.error(msg)
            raise FileNotFoundError(msg)
        data = dict()
        data['path'] = path
        with open(path, encoding=encoding) as fid:
            header = []
            for r, line in enumerate(fid):
                if not line.strip():
                    continue
                split_line = [item.strip() for item in line.split('\t')]
                if r == 0:
                    header = split_line
                    continue
                if len(split_line) != len(header):
                    adm_logger.log_workflow(f'No or insufficient data in analyse_info for parameter: {split_line[0]}', level=adm_logger.WARNING)
                    continue
                line_dict = dict(zip(header, split_line))
                line_dict['VALIDFR'] = _get_date(line_dict['VALIDFR'])
                line_dict['VALIDTO'] = _get_date(line_dict['VALIDTO'])
                par = line_dict['PARAM']
                data.setdefault(par, [])
                data[par].append(line_dict)
        return AnalyseInfo(data, mapper=mapper)

    @classmethod
    def from_lims_txt_file(cls, path: str | pathlib.Path, mapper: Mapper, encoding: str = 'cp1252') -> 'AnalyseInfo':
        path = pathlib.Path(path)
        if path.suffix != '.txt':
            msg = f'File is not a valid analyse_info text file: {path}'
            logger.error(msg)
            raise FileNotFoundError(msg)
        data = dict()
        data['path'] = path
        with open(path, encoding=encoding) as fid:
            header = []
            for r, line in enumerate(fid):
                if not line.strip():
                    continue
                split_line = [item.strip() for item in line.split('\t')]
                if r == 0:
                    header = split_line
                    continue
                if len(split_line) != len(header):
                    adm_logger.log_workflow(f'No or insufficient data in analyse_info for parameter: {split_line[0]}',
                                            level=adm_logger.WARNING)
                    continue
                line_dict = dict(zip(header, split_line))
                line_dict['VALIDFR'] = _get_date(line_dict['VALIDFR (YYYY-MM-DD)'])
                line_dict['VALIDTO'] = _get_date(line_dict['VALIDTO'])
                par = line_dict['PARAM']
                data.setdefault(par, [])
                data[par].append(line_dict)
        return AnalyseInfo(data, mapper=mapper)

    @property
    def data(self) -> dict[str, str]:
        return self._data

    @property
    def parameters(self) -> list[str]:
        """Returns a list of all parameters in the file. The list is unsorted."""
        return list(self._data)

    @property
    def columns(self) -> list[str]:
        return list(self.data[list(self._data)[0]][0])


def _get_date(date_str: str):
    if not date_str:
        return ''
    return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
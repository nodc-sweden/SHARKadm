import functools
import pathlib
import logging

import yaml


logger = logging.getLogger(__name__)

from .data_type_mapper import DataTypeMapper


class ImportMatrixMapper:
    def __init__(self, data_type: str,  import_column: str, data: dict):
        self._data_type = data_type
        self._import_column = import_column
        self._data = data

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self._data_type}" and column "{self._import_column}"'

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def import_column(self) -> str:
        return self._import_column

    def get_internal_name(self, external_par: str) -> str:
        if not self._data.get(external_par):
            logger.warning(f'Could not map parameter "{external_par}" using mapping column "{self.import_column}" for data_type "{self.data_type}"')
            return external_par
        return self._data[external_par].split('.', 1)[-1]


class ImportMatrixConfig:

    def __init__(self,
                 path: str | pathlib.Path,
                 data_type: str = None,
                 encoding: str = 'cp1252',
                 data_type_mapper: DataTypeMapper = None) -> None:
        self._path = pathlib.Path(path)
        self._data_type = data_type_mapper.get(data_type)
        if self._data_type != data_type:
            logger.warning(f'Data type has been mapped: {data_type} -> {self._data_type}')
        self._encoding = encoding
        self._data = {}
        self._mappers = {}

        self._validate()

        self._load_file()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self._data_type}": {self._path}'

    def _validate(self) -> None:
        if self.data_type not in self._path.stem.lower():
            msg = f'Datatype {self.data_type} does not match name of file {self._path.name}'
            logger.error(msg)
            raise ValueError(msg)

    def _load_yaml_file(self) -> None:
        # Not used
        with open(self._path, encoding=self._encoding) as fid:
            self._data = yaml.safe_load(fid)

    def _load_file(self) -> None:
        self._data = {}
        header = []
        with open(self._path, encoding=self._encoding) as fid:
            for r, line in enumerate(fid):
                if not line.strip():
                    continue
                split_line = [item.strip() for item in line.split('\t')]
                if r == 0:
                    header = split_line
                    self._data = {key: {} for key in header[1:]}
                    continue

                for inst, par_str in zip(header[1:], split_line[1:]):
                    if not par_str:
                        continue
                    for par in par_str.split('<or>'):
                        self._data[inst][par.split('.', 1)[-1]] = split_line[0].split('.', 1)[-1]

    @property
    def data_type(self) -> str:
        return self._data_type

    @functools.cached_property
    def institutes(self) -> list:
        """Returns a sorted list of all institutes"""
        return sorted(self._data)

    @functools.cached_property
    def all_external_parameters(self) -> list:
        """Returns a sorted list of all external parameters found in the config file"""
        all_pars = set()
        for par_dict in self._data.values():
            all_pars.update(par_dict.keys())
        return sorted(all_pars)

    @functools.cached_property
    def all_internal_parameters_with_level(self) -> list:
        """Returns a sorted list of all internal parameters found in the config file including the level"""
        all_pars = set()
        for par_dict in self._data.values():
            all_pars.update(par_dict.values())
        return sorted(all_pars)

    @functools.cached_property
    def all_internal_parameters(self) -> list:
        """Returns a sorted list of all internal parameters found in the config file"""
        return sorted([item.split('.', 1)[1] for item in self.all_internal_parameters_with_level])

    @functools.cached_property
    def levels(self) -> list:
        """Returns a sorted list of all levels found in the config file"""
        return sorted(set([item.split('.', 1)[0] for item in self.all_internal_parameters_with_level]))

    def get_mapper(self, import_column: str) -> ImportMatrixMapper:
        """Returns a mapper object for the given institute. Creates it if not found"""
        return self._mappers.setdefault(import_column, ImportMatrixMapper(self.data_type, import_column, self._data[import_column]))

    def get(self, import_column: str, external_par: str) -> str:
        """Returns the internal parameter name for the given institute and external parameter name"""
        return self.get_mapper(import_column).get_internal_name(external_par)


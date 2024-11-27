import functools
import logging
import pathlib

from sharkadm import adm_logger

logger = logging.getLogger(__name__)


class ImportMatrixMapper:
    def __init__(self, data_type: str,  import_column: str, data: dict):
        self._data_type = data_type
        self._import_column = import_column
        self._data = data
        self._reverse_mapper = dict()
        self._create_reverse_mapper()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self._data_type}" and column "{self._import_column}"'

    def _create_reverse_mapper(self):
        self._reverse_mapper = dict()
        for key, value in self._data.items():
            self._reverse_mapper[value] = key
            if 'COPY_VARIABLE' in value:
                self._reverse_mapper[value.split('.')[1]] = key

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def import_column(self) -> str:
        return self._import_column

    @property
    def external_parameters(self) -> list[str]:
        return sorted(self._data)

    def get_internal_name(self, external_par: str) -> str:
        external_par = external_par.strip()
        if not self._data.get(external_par):
            adm_logger.log_workflow(f'Could not map parameter {external_par} using mapping column "{self.import_column}" for '
                      f'data_type "{self.data_type}"', level=adm_logger.DEBUG)
            return external_par
        return self._data[external_par].strip()

    def get_external_name(self, internal_name: str) -> str:
        internal_name = internal_name.strip()
        if not self._reverse_mapper.get(internal_name):
            adm_logger.log_workflow(f'Could not map parameter "{internal_name}" to external name using mapping column '
                                    f'"{self.import_column}" for data_type "{self.data_type}"', level=adm_logger.DEBUG)
            return internal_name
        return self._reverse_mapper[internal_name].strip()


class ImportMatrixConfig:

    def __init__(self,
                 path: str | pathlib.Path,
                 data_type: str = None,
                 encoding: str = 'iso_8859_1') -> None:
                 # data_type_mapper: DataTypeMapper = None) -> None:
        self._path = pathlib.Path(path)
        # self._data_type = data_type_mapper.get(data_type)
        self._data_type = data_type
        if self._data_type != data_type:
            logger.warning(f'Data type has been mapped: {data_type} -> {self._data_type}')
        self._encoding = encoding
        self._data = {}
        self._mappers = {}
        self._columns_by_level = {}

        self._validate()

        self._load_file()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} with data type "{self._data_type}": {self._path}'

    def _validate(self) -> None:
        if self.data_type not in self._path.stem.lower():
            msg = f'Datatype {self.data_type} does not match name of file {self._path.name}'
            logger.error(msg)
            raise ValueError(msg)

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

                self._add_variable_to_level(split_line[0])

                for inst, par_str in zip(header[1:], split_line[1:]):
                    if not par_str:
                        continue
                    for par in par_str.split('<or>'):
                        # self._data[inst][par.split('.', 1)[-1]] = split_line[0].split('.', 1)[-1]
                        self._data[inst][par] = split_line[0].split('.', 1)[-1]

    def _add_variable_to_level(self, item) -> None:
        level, column = item.split('.', 1)
        if 'NOT_USED' in column:
            return
        self._columns_by_level.setdefault(level, [])
        self._columns_by_level[level].append(column)

    @property
    def data_type(self) -> str:
        return self._data_type

    @functools.cached_property
    def institutes(self) -> list:
        """Returns a sorted list of all institutes"""
        return sorted(self._data)

    def get_mapper(self, import_column: str) -> ImportMatrixMapper:
        """Returns a mapper object for the given institute. Creates it if not found"""
        return self._mappers.setdefault(import_column, ImportMatrixMapper(self.data_type, import_column, self._data[import_column]))

    def get(self, import_column: str, external_par: str) -> str:
        """Returns the internal parameter name for the given institute and external parameter name"""
        return self.get_mapper(import_column).get_internal_name(external_par)

    def get_columns_by_level(self) -> dict[str, list[str]]:
        """Returns a dict with levels as key and a list och corresponding variables as value"""
        return self._columns_by_level



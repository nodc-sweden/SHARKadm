import logging
import pathlib
from SHARKadm import adm_logger

logger = logging.getLogger(__name__)


class PhysicalChemicalMapper:

    def __init__(self,
                 path: str | pathlib.Path,
                 encoding: str = 'cp1252') -> None:
        self._path = pathlib.Path(path)
        self._encoding = encoding
        self._data = {}
        self._header = []

        self._load_file()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}'

    def _load_file(self) -> None:
        # self._data = pd.read_csv(self._path, encoding=self._encoding, sep='\t')
        header = []
        with open(self._path) as fid:
            for r, line in enumerate(fid):
                if not line.strip():
                    continue
                split_line = [item.strip() for item in line.split('\t')]
                if r == 0:
                    self._header = split_line
                    continue
                line_dict = dict(zip(self._header, split_line))
                internal_value = line_dict.pop('SHARKadm')
                for value in line_dict.values():
                    self._data[value] = internal_value

    def get_internal_name(self, external_par: str) -> str:
        print('-'*100)
        print(f'{external_par=}')
        if not self._data.get(external_par):
            adm_logger.log_workflow(f'Could not map parameter "{external_par}" using "{self.__class__.__name__}"')
            return external_par
        print(f"{self._data[external_par].split('.', 1)[-1]=}")
        return self._data[external_par].split('.', 1)[-1]

    def get_short_name(self, internal_par: str) -> str:
        pass
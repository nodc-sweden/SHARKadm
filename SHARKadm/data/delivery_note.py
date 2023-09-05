import datetime
import pathlib


class DeliveryNote:

    def __init__(self, path: str | pathlib.Path, encoding='cp1252') -> None:
        self._path: pathlib.Path = pathlib.Path(path)
        self._encoding: str = encoding
        self._data: dict = {}
        self._data_type: str = None
        self._import_matrix_key: str = None

        self._load_file()

    def _load_file(self) -> None:
        with open(self._path, encoding=self._encoding) as fid:
            for line in fid:
                if not line.strip():
                    continue
                key, value = [item.strip() for item in line.split(':', 1)]
                self._data[key] = value
                if key == 'format':
                    parts = [item.strip() for item in value.split(':')]
                    self._data_type = parts[0]
                    self._import_matrix_key = parts[1]

    @property
    def data_type(self) -> str:
        return self._data_type.lower()

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

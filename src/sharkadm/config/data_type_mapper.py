import pathlib
import yaml


class DataTypeMapper:
    def __init__(self, path: str | pathlib.Path) -> None:
        self._path: pathlib.Path = pathlib.Path(path)
        self._data: dict = {}

        self._load_file()

    def _load_file(self) -> None:
        with open(self._path) as fid:
            self._data = yaml.safe_load(fid)

    def get(self, data_type: str, default: str = None) -> str:
        if default is None:
            default = data_type
        return self._data.get(data_type, default)
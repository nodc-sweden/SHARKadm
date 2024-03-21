import pathlib


class DeliveryNoteMapper:
    def __init__(self, path: str | pathlib.Path, encoding: str = 'cp1252') -> None:
        self._path: pathlib.Path = pathlib.Path(path)
        self._encoding = encoding
        self._data = []

        self._load_file()

    def _load_file(self) -> None:
        header = []
        with open(self._path, encoding=self._encoding) as fid:
            for r, line in enumerate(fid):
                if not line.strip():
                    continue
                split_line = [item.strip() for item in line.split('\t')]
                if r == 0:
                    header = split_line
                    continue
                if not all(split_line):
                    continue
                self._data.append(dict(zip(header, split_line)))

    def get_txt_key_from_xlsx_key(self, key):
        for item in self._data:
            if item['xlsx_keys'].lower() == key.lower():
                return item['txt_keys']
        return key

    def get_xlsx_key_from_txt_key(self, key):
        for item in self._data:
            if item['txt_keys'].lower() == key.lower():
                return item['xlsx_keys']
        return key

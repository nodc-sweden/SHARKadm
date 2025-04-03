import pathlib


class DeliveryNoteMapper:
    def __init__(self, path: str | pathlib.Path, encoding: str = "cp1252") -> None:
        self._path: pathlib.Path = pathlib.Path(path)
        self._encoding = encoding
        self._data = {}

        self._load_file()

    def _load_file(self) -> None:
        header = []
        with open(self._path, encoding=self._encoding) as fid:
            for r, line in enumerate(fid):
                if not line.strip():
                    continue
                split_line = [item.strip() for item in line.split("\t")]
                if r == 0:
                    header = split_line
                    continue
                if not all(split_line):
                    continue
                line_data = dict(zip(header, split_line))
                for synonym in line_data["synonyms"].split("<or>"):
                    self._data[synonym.lower()] = line_data["short_key"]
                self._data[line_data["short_key"].lower()] = line_data["short_key"]

    def get(self, synonym: str) -> str:
        return self._data.get(synonym.lower(), synonym)

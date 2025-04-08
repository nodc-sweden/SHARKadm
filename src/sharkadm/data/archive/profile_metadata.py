import logging
import pathlib

from sharkadm.data.archive.shark_metadata import SharkMetadata

logger = logging.getLogger(__name__)


class ProfileMetadata:
    def __init__(self, path: pathlib.Path) -> None:
        self._path = pathlib.Path(path)
        self._load_file()

    def __str__(self):
        lst = []
        for key, value in self._data.items():
            lst.append(f"{key}: {value}")
        return "\n".join(lst)

    def __getitem__(self, item: str) -> str:
        return self._data.get(item)

    def from_txt_file(
        cls, path: str | pathlib.Path, encoding: str = "cp1252"
    ) -> "SharkMetadata":
        path = pathlib.Path(path)
        if path.suffix != ".txt":
            msg = f"File is not a valid shark_metadata text file: {path}"
            logger.error(msg)
            raise FileNotFoundError(msg)
        data = dict()
        data["path"] = path
        with open(path, encoding=encoding) as fid:
            key = None
            for line in fid:
                if not line.strip():
                    continue
                if ":" not in line:
                    # Belongs to previous row
                    data[key] = f"{data[key]} {line.strip()}"
                    continue
                key, value = [item.strip() for item in line.split(":", 1)]
                key = key.lstrip("- ")
                data[key] = value
        return SharkMetadata(data)

    @property
    def data(self) -> dict[str, str]:
        return self._data

    @property
    def fields(self) -> list[str]:
        """Returns a list of all the fields in the file. The list is unsorted."""
        return list(self._data)

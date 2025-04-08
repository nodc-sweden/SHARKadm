import pathlib


class ChangeLog:
    def __init__(self, path: str | pathlib.Path, encoding="cp1252") -> None:
        self._path = pathlib.Path(path)
        if self._path.name != "change_log.txt":
            raise NameError('Change log can only be named "change_log.txt"')
        self._encoding = encoding
        self._data = []

        self._load_file()

    def _load_file(self) -> None:
        with open(self._path, encoding=self._encoding) as fid:
            for line in fid:
                if not line.strip():
                    continue
                self._data.append(line)

    def get_log_lines(self) -> list[str]:
        return self._data

    def get_log_as_text(self) -> str:
        return "\n".join(self._data)

    def add_to_log(self, text: str, save_file: bool = False) -> None:
        """Adds given text to end of log"""
        if text in self._data:
            return
        self._data.append(text)
        if save_file:
            self._save_file()

    def _save_file(self) -> None:
        """Saves the file to the original destination"""
        with open(self._path, "w", encoding=self._encoding) as fid:
            fid.write("\n".join(self._data))

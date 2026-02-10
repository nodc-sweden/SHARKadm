import pathlib

from sharkadm.sharkadm_logger import adm_logger


class ChangeLog:
    def __init__(self, path: str | pathlib.Path, encoding="cp1252") -> None:
        self._path = pathlib.Path(path)
        # if self._path.name != "change_log.txt":
        #     raise NameError('Change log can only be named "change_log.txt"')
        self._encoding = encoding
        self._data = []
        self._sharkadm_logger_info = []

        self._load_file()

    def _load_file(self) -> None:
        if not self._path.exists():
            return
        with open(self._path, encoding=self._encoding) as fid:
            for line in fid:
                strip_line = line.strip()
                if not strip_line:
                    continue
                self._data.append(strip_line)

    def get_log_lines(self) -> list[str]:
        return [*self._data, "", *self._sharkadm_logger_info]

    def get_log_as_text(self) -> str:
        return "\n".join(self.get_log_lines())

    def add_to_log(self, text: str) -> None:
        """Adds given text to end of log"""
        if text in self._data:
            return
        self._data.append(text)

    def add_sharkadm_logger_info(self):
        self._sharkadm_logger_info = []
        adm_logger.reset_filter()
        adm_logger.filter(levels="info", log_type="transformation")
        for data in adm_logger.data:
            self._sharkadm_logger_info.append(data.get("msg", ""))
        adm_logger.reset_filter()

    def save_file(self, path: pathlib.Path | str) -> None:
        """Saves the file to the original destination"""
        path = pathlib.Path(path)
        if path.exists():
            raise FileExistsError(path)
        with open(path, "w", encoding=self._encoding) as fid:
            fid.write(self.get_log_as_text())

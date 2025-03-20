import datetime
import logging
import pathlib
from typing import TYPE_CHECKING

from sharkadm.utils.paths import get_next_incremented_file_path
from .base import SharkadmLoggerExporter

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


class TxtExporter(SharkadmLoggerExporter):

    def __init__(self, **kwargs):
        self._add_to_file: pathlib.Path | None = None
        if kwargs.get('add_to_file'):
            self._add_to_file = pathlib.Path(kwargs.get('add_to_file'))
        super().__init__(**kwargs)

    @property
    def path(self) -> pathlib.Path:
        return self._add_to_file or self.file_path

    def _get_default_file_name(self):
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        data_string = '-'.join(list(self.adm_logger.data()))
        file_name = f'sharkadm_log_{self.adm_logger.name}_{date_str}_{data_string}'
        return file_name

    def _export(self) -> None:
        self._set_save_path(suffix='.txt')
        info = self._extract_info()
        try:
            self._save_as_txt(info, self.path)
            logger.info(f'Saving sharkadm txt log to {self.path}')
        except PermissionError:
            path = get_next_incremented_file_path(self.path)
            self._save_as_txt(info, path)
            logger.info(f'Saving sharkadm txt log to {path}')

    def _extract_info(self) -> list[str]:
        info = []
        for data in self.adm_logger.data:
            line_list = [
                data.get('msg', ''),
                data.get('level', ''),
            ]
            info.append('\t'.join(line_list))
        return info

    @staticmethod
    def _save_as_txt(info: list, path: pathlib.Path):
        with open(path, 'a') as fid:
            fid.write('\n'.join(info))

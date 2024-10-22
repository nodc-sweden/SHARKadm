import datetime
import logging
from typing import TYPE_CHECKING

from sharkadm.utils.paths import get_next_incremented_file_path
from .base import SharkadmLoggerExporter

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


class TxtExporter(SharkadmLoggerExporter):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_default_file_name(self):
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        data_string = '-'.join(list(self.adm_logger.data.keys()))
        file_name = f'sharkadm_log_{self.adm_logger.name}_{date_str}_{data_string}'
        return file_name

    def _export(self) -> None:
        self._set_save_path(suffix='.txt')
        info = self._extract_info()
        try:
            self._save_as_txt(info)
            logger.info(f'Saving sharkadm xlsx log to {self.file_path}')
        except PermissionError:
            self.file_path = get_next_incremented_file_path(self.file_path)
            self._save_as_txt(info)
            logger.info(f'Saving sharkadm xlsx log to {self.file_path}')

    def _extract_info(self) -> list:
        info = []
        for level, level_data in self.adm_logger.data.items():
            for purpose, purpose_data in level_data.items():
                for log_type, log_type_data in purpose_data.items():
                    for msg, msg_data in log_type_data.items():
                        line_list = [
                            msg,
                            level
                        ]
                        info.append('\t'.join(line_list))
        return info

    def _save_as_txt(self, info: list):
        with open(self.file_path, 'w') as fid:
            fid.write('\n'.join(info))
import logging
import pathlib
from abc import abstractmethod, ABC
from typing import TYPE_CHECKING

from sharkadm import utils

if TYPE_CHECKING:
    from .sharkadm_logger import SHARKadmLogger


logger = logging.getLogger(__name__)


class SharkadmLoggerExporter(ABC):

    def __init__(self, **kwargs):
        self.adm_logger: 'SHARKadmLogger' | None = None
        self.file_path: pathlib.Path | str | None = None
        self.kwargs = kwargs

    def export(self, adm_logger: 'SHARKadmLogger'):
        self.adm_logger = adm_logger
        self._export()
        self._open_file()
        self._open_directory()

    @abstractmethod
    def _export(self):
        ...

    @abstractmethod
    def _get_default_file_name(self):
        ...

    def _set_save_path(self, suffix):
        file_path = self.kwargs.get('export_file_path') or self.kwargs.get('file_path')
        file_name = self.kwargs.get('export_file_name') or self.kwargs.get('file_name') or self._get_default_file_name()
        export_directory = self.kwargs.get('export_directory') or self.kwargs.get('directory')
        if file_path:
            self.file_path = file_path
        else:
            if not export_directory:
                export_directory = utils.get_export_directory()
            print(export_directory)
            print(file_name)
            self.file_path = pathlib.Path(export_directory, file_name)
        if self.file_path.suffix != suffix:
            self.file_path = self.file_path.with_suffix(suffix)
            # self.file_path = pathlib.Path(str(self.file_path) + f'.{suffix.strip('.')}')
        if not self.file_path.parent.exists():
            raise NotADirectoryError(self.file_path.parent)

    def _open_directory(self):
        if not self.kwargs.get('open_directory', self.kwargs.get('open_export_directory')):
            return
        if not self.file_path:
            logger.info(f'open_directory is not implemented for exporter {self.__class__.__name__}')
            return
        utils.open_directory(self.file_path.parent)

    def _open_file(self):
        if not self.kwargs.get('open_report', self.kwargs.get('open_file', self.kwargs.get('open_export_file'))):
            return
        if not self.file_path:
            logger.info(f'open_file is not implemented for exporter {self.__class__.__name__}')
            return
        utils.open_file_with_default_program(self.file_path)





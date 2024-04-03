import pathlib
import time
from abc import ABC, abstractmethod
from typing import Protocol

import pandas as pd

from sharkadm import adm_logger, config
from sharkadm.data import get_valid_data_holders
from sharkadm import utils


class DataHolderProtocol(Protocol):

    @property
    @abstractmethod
    def data(self) -> pd.DataFrame:
        ...

    @property
    @abstractmethod
    def data_type(self) -> str:
        ...

    @property
    @abstractmethod
    def dataset_name(self) -> str:
        ...


class Exporter(ABC):
    """Abstract base class used as a blueprint for exporting stuff in a DataHolder"""
    valid_data_types = []
    invalid_data_types = []

    valid_data_holders = []
    invalid_data_holders = []

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f'Exporter: {self.__class__.__name__}'

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_exporter_description() -> str:
        """Verbal description describing what the exporter is doing"""
        ...

    @property
    def description(self) -> str:
        return self.get_exporter_description()
    
    def export(self, data_holder: DataHolderProtocol) -> None:
        if data_holder.data_type.lower() not in config.get_valid_data_types(valid=self.valid_data_types,
                                                                            invalid=self.invalid_data_types):
            adm_logger.log_workflow(f'Invalid data_type {data_holder.data_type} for exporter {self.__class__.__name__}', level=adm_logger.DEBUG)
            return
        if data_holder.__class__.__name__ not in get_valid_data_holders(valid=self.valid_data_holders,
                                                                        invalid=self.invalid_data_holders):
            adm_logger.log_workflow(f'Invalid data_holder {data_holder.__class__.__name__} for exporter'
                                    f' {self.__class__.__name__}')
            return

        adm_logger.log_workflow(f'Applying exporter: {self.__class__.__name__}', add=self.get_exporter_description())
        t0 = time.time()
        self._export(data_holder=data_holder)
        adm_logger.log_workflow(f'Exporter {self.__class__.__name__} executed in {time.time() - t0} seconds')

    @abstractmethod
    def _export(self, data_holder: DataHolderProtocol) -> None:
        ...


class FileExporter(Exporter, ABC):
    def __init__(self,
                 export_directory: str | pathlib.Path | None = None,
                 export_file_name: str | pathlib.Path | None = None,
                 **kwargs):
        super().__init__(**kwargs)
        if not export_directory:
            export_directory = utils.get_export_directory()
        self._export_directory = pathlib.Path(export_directory)
        self._export_file_name = export_file_name
        self._encoding = kwargs.get('encoding', 'cp1252')

    @property
    def export_file_path(self):
        return pathlib.Path(self._export_directory, self._export_file_name)

    @property
    def export_directory(self):
        return self.export_file_path.parent

    def export(self, data_holder: DataHolderProtocol):
        super().export(data_holder=data_holder)
        self.open_file()
        self.open_file_with_excel()
        self.open_directory()

    def open_file(self):
        if self._kwargs.get('open_file', self._kwargs.get('open_export_file')) and self.export_file_path:
            utils.open_file_with_default_program(self.export_file_path)
        return self

    def open_file_with_excel(self):
        if self._kwargs.get('open_file_with_excel', self._kwargs.get('open_export_file_with_excel', self._kwargs.get('open_with_excel'))) and self.export_file_path:
            utils.open_file_with_excel(self.export_file_path)
        return self

    def open_directory(self):
        if self._kwargs.get('open_directory') and self.export_file_path:
            utils.open_directory(self.export_file_path.parent)
        return self

    @staticmethod
    @abstractmethod
    def get_exporter_description() -> str:
        """Verbal description describing what the exporter is doing"""
        ...

    @abstractmethod
    def _export(self, data_holder: DataHolderProtocol) -> None:
        ...


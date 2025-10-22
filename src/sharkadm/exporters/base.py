import pathlib
import time
from abc import ABC, abstractmethod
from typing import Any, Protocol

import pandas as pd

from sharkadm import config, utils
from sharkadm.data import PolarsDataHolder, is_valid_data_holder
from sharkadm.sharkadm_logger import adm_logger


class DataHolderProtocol(Protocol):
    @property
    @abstractmethod
    def data(self) -> pd.DataFrame: ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...

    @property
    @abstractmethod
    def data_type_internal(self) -> str: ...

    @property
    @abstractmethod
    def dataset_name(self) -> str: ...


class Exporter(ABC):
    """Abstract base class used as a blueprint for exporting stuff in a DataHolder"""

    valid_data_types = ()
    invalid_data_types = ()

    valid_data_holders = ()
    invalid_data_holders = ()

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"Exporter: {self.__class__.__name__}"

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

    def export(self, data_holder: DataHolderProtocol) -> Any:
        if (
            data_holder.data_type_internal != "unknown"
            and data_holder.data_type_internal
            not in config.get_valid_data_types(
                valid=self.valid_data_types, invalid=self.invalid_data_types
            )
        ):
            adm_logger.log_workflow(
                f"Invalid data_type {data_holder.data_type_internal} "
                f"for exporter {self.__class__.__name__}",
                level=adm_logger.DEBUG,
            )
            return

        if not is_valid_data_holder(
            data_holder,
            valid=self.valid_data_holders,
            invalid=self.invalid_data_holders,
        ):
            adm_logger.log_workflow(
                f"Invalid data_holder {data_holder.__class__.__name__} for exporter"
                f" {self.__class__.__name__}"
            )
            return

        adm_logger.log_workflow(
            f"Applying exporter: {self.__class__.__name__}",
            item=self.get_exporter_description(),
            level=adm_logger.DEBUG,
        )
        t0 = time.time()
        data = self._export(data_holder=data_holder)
        adm_logger.log_workflow(
            f"Exporter {self.__class__.__name__} executed in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )
        return data

    @abstractmethod
    def _export(self, data_holder: DataHolderProtocol) -> None: ...

    def _log(self, msg: str, **kwargs):
        adm_logger.log_export(msg, cls=self.__class__.__name__, **kwargs)


class PolarsExporter(ABC):
    """Abstract base class used as a blueprint for exporting stuff in a DataHolder"""

    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    source_col: str = ""
    col_to_set: str = ""

    def __init__(
        self,
        valid_data_types: tuple[str, ...] = (),
        invalid_data_types: tuple[str, ...] = (),
        valid_data_holders: tuple[str, ...] = (),
        invalid_data_holders: tuple[str, ...] = (),
        valid_data_structures: tuple[str, ...] = (),
        invalid_data_structures: tuple[str, ...] = (),
        **kwargs,
    ):
        self.valid_data_types = valid_data_types or self.valid_data_types
        self.invalid_data_types = invalid_data_types or self.invalid_data_types

        self.valid_data_holders = valid_data_holders or self.valid_data_holders
        self.invalid_data_holders = invalid_data_holders or self.invalid_data_holders

        self.valid_data_structures = valid_data_structures or self.valid_data_structures
        self.invalid_data_structures = (
            invalid_data_structures or self.invalid_data_structures
        )

        self._kwargs = kwargs

    def __repr__(self) -> str:
        return f"Exporter: {self.__class__.__name__}"

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

    def export(self, data_holder: PolarsDataHolder) -> Any:
        if (
            data_holder.data_type_internal != "unknown"
            and data_holder.data_type_internal
            not in config.get_valid_data_types(
                valid=self.valid_data_types, invalid=self.invalid_data_types
            )
        ):
            adm_logger.log_workflow(
                f"Invalid data_type {data_holder.data_type_internal} "
                f"for exporter {self.__class__.__name__}",
                level=adm_logger.DEBUG,
            )
            return

        if not is_valid_data_holder(
            data_holder,
            valid=self.valid_data_holders,
            invalid=self.invalid_data_holders,
        ):
            adm_logger.log_workflow(
                f"Invalid data_holder {data_holder.__class__.__name__} for exporter"
                f" {self.__class__.__name__}"
            )
            return

        adm_logger.log_workflow(
            f"Applying exporter: {self.__class__.__name__}",
            item=self.get_exporter_description(),
            level=adm_logger.DEBUG,
        )
        t0 = time.time()
        data = self._export(data_holder=data_holder)
        adm_logger.log_workflow(
            f"Exporter {self.__class__.__name__} executed in {time.time() - t0} seconds",
            level=adm_logger.DEBUG,
        )
        return data

    @abstractmethod
    def _export(self, data_holder: PolarsDataHolder) -> None: ...

    def _log(self, msg: str, **kwargs):
        adm_logger.log_export(msg, cls=self.__class__.__name__, **kwargs)

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)


class FileExporter(Exporter, ABC):
    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not export_directory:
            export_directory = utils.get_export_directory()
        self._export_directory = pathlib.Path(export_directory)
        self._export_directory.mkdir(parents=True, exist_ok=True)
        self._export_file_name = export_file_name
        self._encoding = kwargs.get("encoding", "cp1252")

    @property
    def export_directory(self):
        return self._export_directory

    @property
    def export_file_name(self):
        return self._export_file_name

    @property
    def export_file_path(self):
        if not (self.export_directory and self.export_file_name):
            return
        return pathlib.Path(self._export_directory, self._export_file_name)

    def export(self, data_holder: DataHolderProtocol):
        super().export(data_holder=data_holder)
        self.open_file()
        self.open_file_with_excel()
        self.open_directory()

    def open_file(self):
        if not self.export_file_name:
            return
        if (
            self._kwargs.get(
                "open_file",
                self._kwargs.get("open_export_file", self._kwargs.get("open")),
            )
            and self.export_file_path
        ):
            utils.open_file_with_default_program(self.export_file_path)
        return self

    def open_file_with_excel(self):
        if not self.export_file_name:
            return
        if self.export_file_path and any(
            [
                self._kwargs.get("open_file_with_excel"),
                self._kwargs.get("open_export_file_with_excel"),
                self._kwargs.get("open_with_excel"),
                self._kwargs.get("open_file_in_excel"),
                self._kwargs.get("open_export_file_in_excel"),
                self._kwargs.get("open_in_excel"),
            ]
        ):
            utils.open_file_with_excel(self.export_file_path)
        return self

    def open_directory(self):
        if self._kwargs.get("open_directory") and self.export_directory:
            utils.open_file_or_directory(self.export_directory)
        return self

    @staticmethod
    @abstractmethod
    def get_exporter_description() -> str:
        """Verbal description describing what the exporter is doing"""
        ...

    @abstractmethod
    def _export(self, data_holder: DataHolderProtocol) -> None: ...


class PolarsFileExporter(PolarsExporter, ABC):
    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not export_directory:
            export_directory = utils.get_export_directory()
        self._export_directory = pathlib.Path(export_directory)
        self._export_directory.mkdir(parents=True, exist_ok=True)
        self._export_file_name = export_file_name
        self._encoding = kwargs.get("encoding", "cp1252")

    @property
    def export_directory(self):
        return self._export_directory

    @property
    def export_file_name(self):
        return self._export_file_name

    @property
    def export_file_path(self):
        if not (self.export_directory and self.export_file_name):
            return
        return pathlib.Path(self._export_directory, self._export_file_name)

    def export(self, data_holder: PolarsDataHolder):
        super().export(data_holder=data_holder)
        self.open_file()
        self.open_file_with_excel()
        self.open_directory()

    def open_file(self):
        if not self.export_file_name:
            return
        if (
            self._kwargs.get(
                "open_file",
                self._kwargs.get("open_export_file", self._kwargs.get("open")),
            )
            and self.export_file_path
        ):
            utils.open_file_with_default_program(self.export_file_path)
        return self

    def open_file_with_excel(self):
        if not self.export_file_name:
            return
        if self.export_file_path and any(
            [
                self._kwargs.get("open_file_with_excel"),
                self._kwargs.get("open_export_file_with_excel"),
                self._kwargs.get("open_with_excel"),
                self._kwargs.get("open_file_in_excel"),
                self._kwargs.get("open_export_file_in_excel"),
                self._kwargs.get("open_in_excel"),
            ]
        ):
            utils.open_file_with_excel(self.export_file_path)
        return self

    def open_directory(self):
        if self._kwargs.get("open_directory") and self.export_directory:
            utils.open_file_or_directory(self.export_directory)
        return self

    @staticmethod
    @abstractmethod
    def get_exporter_description() -> str:
        """Verbal description describing what the exporter is doing"""
        ...

    @abstractmethod
    def _export(self, data_holder: PolarsDataHolder) -> None: ...

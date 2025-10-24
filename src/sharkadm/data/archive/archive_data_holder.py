import logging
import pathlib
from abc import ABC, abstractmethod

import pandas as pd
import polars as pl

from sharkadm import config
from sharkadm.config.import_matrix import ImportMatrixConfig, ImportMatrixMapper
from sharkadm.data.archive import analyse_info, delivery_note, metadata, sampling_info
from sharkadm.data.data_holder import PandasDataHolder, PolarsDataHolder
from sharkadm.data.data_source.base import DataFile, PolarsDataFile
from sharkadm.data.data_source.txt_file import (
    CsvRowFormatPolarsDataFile,
)
from sharkadm.sharkadm_logger import adm_logger

logger = logging.getLogger(__name__)


class ArchiveDataHolder(PandasDataHolder, ABC):
    _data_type: str | None = None
    _data_type_internal: str | None = None
    _data_format: str | None = None

    _date_str_format = "%Y-%m-%d"

    def __init__(
        self, archive_root_directory: str | pathlib.Path | None = None, **kwargs
    ):
        super().__init__()
        self._archive_root_directory = pathlib.Path(archive_root_directory)

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._delivery_note: delivery_note.DeliveryNote | None = None
        self._sampling_info: sampling_info.SamplingInfo | None = None
        self._analyse_info: analyse_info.AnalyseInfo | None = None
        self._import_matrix: ImportMatrixConfig | None = None
        self._import_matrix_mapper: ImportMatrixMapper | None = None
        # self._data_type_mapper = get_data_type_mapper()

        self._data_sources: dict[str, DataFile] = {}

        self._initiate()
        self._load_delivery_note()
        self._load_import_matrix()
        self._load_sampling_info()
        self._load_analyse_info()
        self._load_data()
        self._load_delivery_note()  # Reload to map

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from an archive"""

    def _initiate(self) -> None:
        self._dataset_name = self.archive_root_directory.name

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def data_format(self) -> str:
        return self._data_format

    @property
    def header_mapper(self):
        # TODO: Change
        return self._import_matrix_mapper

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def data_type_internal(self) -> str:
        # return self._data_type_mapper.get(self.data_format)
        return self._data_type_internal

    @property
    def delivery_note(self) -> delivery_note.DeliveryNote:
        return self._delivery_note

    @property
    def sampling_info(self) -> sampling_info.SamplingInfo:
        return self._sampling_info

    @property
    def analyse_info(self) -> analyse_info.AnalyseInfo:
        return self._analyse_info

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def reporting_institute(self) -> str:
        return self._delivery_note.reporting_institute_code

    @property
    def archive_root_directory(self) -> pathlib.Path:
        return self._archive_root_directory

    @property
    def received_data_directory(self) -> pathlib.Path:
        return self.archive_root_directory / "received_data"

    @property
    def received_data_files(self) -> list[pathlib.Path]:
        paths = []
        if not self.received_data_directory.exists():
            adm_logger.log_workflow('"Received" folder not found', level=adm_logger.INFO)
            return paths
        for path in self.received_data_directory.iterdir():
            if path.is_dir():
                continue
            paths.append(path)
        return paths

    @property
    def processed_data_directory(self) -> pathlib.Path:
        return self.archive_root_directory / "processed_data"

    @property
    def processed_data_files(self) -> list[pathlib.Path]:
        paths = [
            self.delivery_note_path,
            self.sampling_info_path,
            self.analyse_info_path,
        ]
        for name, source in self._data_sources.items():
            print(f"{source.path=}")
            paths.append(source.path)
        return paths

    @property
    def delivery_note_path(self) -> pathlib.Path:
        return self.processed_data_directory / "delivery_note.txt"

    @property
    def sampling_info_path(self) -> pathlib.Path:
        return self.processed_data_directory / "sampling_info.txt"

    @property
    def analyse_info_path(self) -> pathlib.Path:
        return self.processed_data_directory / "analyse_info.txt"

    @property
    def shark_metadata_path(self) -> pathlib.Path:
        return self.archive_root_directory / "shark_metadata.txt"

    @property
    def import_matrix(self) -> ImportMatrixConfig:
        return self._import_matrix

    @property
    def import_matrix_mapper(self) -> ImportMatrixMapper:
        return self._import_matrix_mapper

    @property
    def min_year(self) -> str:
        return str(min(self.data["datetime"]).year)

    @property
    def max_year(self) -> str:
        return str(max(self.data["datetime"]).year)

    @property
    def min_date(self) -> str:
        return min(self.data["datetime"]).strftime(self._date_str_format)

    @property
    def max_date(self) -> str:
        return max(self.data["datetime"]).strftime(self._date_str_format)

    @property
    def min_longitude(self) -> str:
        return str(min(self.data["visit_reported_longitude"].astype(float)))

    @property
    def max_longitude(self) -> str:
        return str(max(self.data["visit_reported_longitude"].astype(float)))

    @property
    def min_latitude(self) -> str:
        return str(min(self.data["visit_reported_latitude"].astype(float)))

    @property
    def max_latitude(self) -> str:
        return str(max(self.data["visit_reported_latitude"].astype(float)))

    def _load_delivery_note(self) -> None:
        self._delivery_note = delivery_note.DeliveryNote.from_txt_file(
            self.delivery_note_path, mapper=self._import_matrix_mapper
        )

    def _load_sampling_info(self) -> None:
        if not self.sampling_info_path.exists():
            adm_logger.log_workflow(
                f"No sampling info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._sampling_info = sampling_info.SamplingInfo.from_txt_file(
            self.sampling_info_path, mapper=self._import_matrix_mapper
        )

    def _load_analyse_info(self) -> None:
        if not self.analyse_info_path.exists():
            adm_logger.log_workflow(
                f"No analyse info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._analyse_info = analyse_info.AnalyseInfo.from_txt_file(
            self.analyse_info_path, mapper=self._import_matrix_mapper
        )

    def _load_import_matrix(self) -> None:
        """Loads the import matrix for the given data type and provider found in
        delivery note"""
        self._import_matrix = config.get_import_matrix_config(
            data_type=self.data_type_internal
        )
        # if not self._import_matrix:
        #     self._import_matrix = config.get_import_matrix_config(
        #         data_type=self.delivery_note.data_format
        #     )
        self._import_matrix_mapper = self._import_matrix.get_mapper(
            self.delivery_note.import_matrix_key
        )

    def _add_concatenated_column(
        self, new_column: str, columns_to_use: list[str]
    ) -> None:
        """Adds a concatenated column specified in new_column using the columns listed in
        columns_to_use"""
        for col in columns_to_use:
            if new_column not in self._data.columns:
                self._data[new_column] = self._data[col]
            else:
                self._data[new_column] = self._data[new_column] + self._data[col]

    def _check_data_source(self, data_source: DataFile) -> None:
        return
        # if (
        #     self._data_type_mapper.get(data_source.data_type) !=
        #     self._data_type_mapper.get(self.data_type)
        # ):
        if data_source.data_type.lower() != self.data_type.lower().replace("_", ""):
            msg = (
                f"Data source {data_source} in data holder {self.name} "
                f"is not of type {self.data_type}"
            )
            logger.error(msg)
            raise ValueError(msg)

    @abstractmethod
    def _load_data(self): ...


class PolarsArchiveDataHolder(PolarsDataHolder, ABC):
    _data_type: str | None = None
    _data_type_internal: str | None = None
    _data_format: str | None = None
    _data_structure = "column"

    _date_str_format = "%Y-%m-%d"

    def __init__(
        self, archive_root_directory: str | pathlib.Path | None = None, **kwargs
    ):
        super().__init__()
        self._archive_root_directory = pathlib.Path(archive_root_directory)

        self._data: pl.DataFrame = pl.DataFrame()
        self._dataset_name: str | None = None

        self._delivery_note: delivery_note.DeliveryNote | None = None
        self._sampling_info: sampling_info.SamplingInfo | None = None
        self._analyse_info: analyse_info.AnalyseInfo | None = None
        self._metadata: metadata.Metadata | None = None
        self._import_matrix: ImportMatrixConfig | None = None
        self._import_matrix_mapper: ImportMatrixMapper | None = None
        # self._data_type_mapper = get_data_type_mapper()

        self._data_sources: dict[str, DataFile] = {}

        self._initiate()
        self._load_delivery_note()
        self._load_import_matrix()
        self._load_sampling_info()
        self._load_analyse_info()
        self._load_metadata()
        self._load_data()
        self._load_delivery_note()  # Reload to map

    def _load_data(self) -> None:
        data_file_path = self.processed_data_directory / "data.txt"
        if not data_file_path.exists():
            logger.info(
                f"No data file found in {self.processed_data_directory}. "
                f"Looking for file with keyword 'data'..."
            )
            for path in self.processed_data_directory.iterdir():
                if "data" in path.stem and path.suffix == ".txt":
                    data_file_path = path
                    logger.info(f"Will use data file: {path}")
                    break
        if not data_file_path:
            logger.error(
                f"Could not find any data file in delivery: {self.archive_root_directory}"
            )
            return

        d_source = CsvRowFormatPolarsDataFile(
            path=data_file_path, data_type=self.delivery_note.data_type
        )
        d_source.map_header(self.import_matrix_mapper)

        self._set_data_source(d_source)

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from an archive"""

    def _initiate(self) -> None:
        self._dataset_name = self.archive_root_directory.name

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def data_format(self) -> str:
        return self._data_format

    @property
    def header_mapper(self):
        # TODO: Change
        return self._import_matrix_mapper

    @property
    def data_type(self) -> str:
        return self._data_type

    @property
    def data_type_internal(self) -> str:
        # return self._data_type_mapper.get(self.data_format)
        return self._data_type_internal

    @property
    def delivery_note(self) -> delivery_note.DeliveryNote:
        return self._delivery_note

    @property
    def sampling_info(self) -> sampling_info.SamplingInfo:
        return self._sampling_info

    @property
    def analyse_info(self) -> analyse_info.AnalyseInfo:
        return self._analyse_info

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def reporting_institute(self) -> str:
        return self._delivery_note.reporting_institute_code

    @property
    def archive_root_directory(self) -> pathlib.Path:
        return self._archive_root_directory

    @property
    def received_data_directory(self) -> pathlib.Path:
        return self.archive_root_directory / "received_data"

    @property
    def received_data_files(self) -> list[pathlib.Path]:
        paths = []
        if not self.received_data_directory.exists():
            adm_logger.log_workflow('"Received" folder not found', level=adm_logger.INFO)
            return paths
        for path in self.received_data_directory.iterdir():
            if path.is_dir():
                continue
            paths.append(path)
        return paths

    @property
    def processed_data_directory(self) -> pathlib.Path:
        return self.archive_root_directory / "processed_data"

    @property
    def processed_data_files(self) -> list[pathlib.Path]:
        paths = [
            self.delivery_note_path,
            self.sampling_info_path,
            self.analyse_info_path,
        ]
        for name, source in self._data_sources.items():
            paths.append(source.path)
        return paths

    @property
    def delivery_note_path(self) -> pathlib.Path:
        return self.processed_data_directory / "delivery_note.txt"

    @property
    def sampling_info_path(self) -> pathlib.Path:
        return self.processed_data_directory / "sampling_info.txt"

    @property
    def analyse_info_path(self) -> pathlib.Path:
        return self.processed_data_directory / "analyse_info.txt"

    @property
    def metadata_path(self) -> pathlib.Path:
        return self.processed_data_directory / "metadata.txt"

    @property
    def shark_metadata_path(self) -> pathlib.Path:
        return self.archive_root_directory / "shark_metadata.txt"

    @property
    def import_matrix(self) -> ImportMatrixConfig:
        return self._import_matrix

    @property
    def import_matrix_mapper(self) -> ImportMatrixMapper:
        return self._import_matrix_mapper

    @property
    def min_year(self) -> str:
        return str(min(self.data["datetime"]).year)

    @property
    def max_year(self) -> str:
        return str(max(self.data["datetime"]).year)

    @property
    def min_date(self) -> str:
        return min(self.data["datetime"]).strftime(self._date_str_format)

    @property
    def max_date(self) -> str:
        return max(self.data["datetime"]).strftime(self._date_str_format)

    @property
    def min_longitude(self) -> str:
        return str(min(self.data["visit_reported_longitude"].cast(float)))

    @property
    def max_longitude(self) -> str:
        return str(max(self.data["visit_reported_longitude"].cast(float)))

    @property
    def min_latitude(self) -> str:
        return str(min(self.data["visit_reported_latitude"].cast(float)))

    @property
    def max_latitude(self) -> str:
        return str(max(self.data["visit_reported_latitude"].cast(float)))

    def _load_delivery_note(self) -> None:
        self._delivery_note = delivery_note.DeliveryNote.from_txt_file(
            self.delivery_note_path, mapper=self._import_matrix_mapper
        )

    def _load_sampling_info(self) -> None:
        if not self.sampling_info_path.exists():
            adm_logger.log_workflow(
                f"No sampling info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._sampling_info = sampling_info.SamplingInfo.from_txt_file(
            self.sampling_info_path, mapper=self._import_matrix_mapper
        )

    def _load_analyse_info(self) -> None:
        if not self.analyse_info_path.exists():
            adm_logger.log_workflow(
                f"No analyse info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._analyse_info = analyse_info.AnalyseInfo.from_txt_file(
            self.analyse_info_path, mapper=self._import_matrix_mapper
        )

    def _load_metadata(self) -> None:
        if not self.metadata_path.exists():
            adm_logger.log_workflow(
                f"No metadata file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._metadata = metadata.Metadata.from_txt_file(
            self.metadata_path, mapper=self._import_matrix_mapper
        )

    def _load_import_matrix(self) -> None:
        """Loads the import matrix for the given data type and provider found in
        delivery note"""
        self._import_matrix = config.get_import_matrix_config(
            data_type=self.data_type_internal
        )
        # if not self._import_matrix:
        #     self._import_matrix = config.get_import_matrix_config(
        #         data_type=self.delivery_note.data_format
        #     )
        self._import_matrix_mapper = self._import_matrix.get_mapper(
            self.delivery_note.import_matrix_key
        )

    def _add_concatenated_column(
        self, new_column: str, columns_to_use: list[str]
    ) -> None:
        """Adds a concatenated column specified in new_column using the columns listed
        in columns_to_use"""
        for col in columns_to_use:
            if new_column not in self._data.columns:
                self._data[new_column] = self._data[col]
            else:
                self._data[new_column] = self._data[new_column] + self._data[col]

    def _check_data_source(self, data_source: PolarsDataFile) -> None:
        return
        # if (
        #     self._data_type_mapper.get(data_source.data_type) !=
        #     self._data_type_mapper.get(self.data_type
        # ):
        if data_source.data_type.lower() != self.data_type.lower().replace("_", ""):
            msg = (
                f"Data source {data_source} in data holder {self.name} "
                f"is not of type {self.data_type}"
            )
            logger.error(msg)
            raise ValueError(msg)

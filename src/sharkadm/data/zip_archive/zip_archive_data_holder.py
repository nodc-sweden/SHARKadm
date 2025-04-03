import logging
import os
import pathlib
import shutil
from abc import ABC, abstractmethod

import pandas as pd

from sharkadm import config
from sharkadm.config import mapper_data_type_to_internal

# from sharkadm.config import get_data_type_mapper
from sharkadm.config.import_matrix import ImportMatrixConfig
from sharkadm.config.import_matrix import ImportMatrixMapper
from sharkadm.data import data_source
from sharkadm.data.archive import delivery_note, analyse_info
from sharkadm.data.archive import sampling_info
from sharkadm.data.data_holder import PandasDataHolder
from sharkadm import adm_logger
from sharkadm import utils

logger = logging.getLogger(__name__)


class ZipArchiveDataHolder(PandasDataHolder, ABC):
    _data_type_internal: str | None = None
    _data_type: str | None = None
    _data_format: str | None = None
    _data_structure = "row"

    _date_str_format = "%Y-%m-%d"

    def __init__(self, zip_archive_path: str | pathlib.Path = None, **kwargs):
        super().__init__()
        self._zip_archive_path = pathlib.Path(zip_archive_path)

        self._data: pd.DataFrame = pd.DataFrame()
        self._dataset_name: str | None = None

        self._delivery_note: delivery_note.DeliveryNote | None = None
        self._sampling_info: sampling_info.SamplingInfo | None = None
        self._analyse_info: analyse_info.AnalyseInfo | None = None
        self._import_matrix: ImportMatrixConfig | None = None
        self._import_matrix_mapper: ImportMatrixMapper | None = None
        # self._data_type_mapper = get_data_type_mapper()

        self._data_sources = {}

        self._initiate()
        self._unzip_archive()
        self._load_delivery_note()
        self._load_sampling_info()
        self._load_analyse_info()
        self._load_data()

    @staticmethod
    def get_data_holder_description() -> str:
        return """Holds data from an archive"""

    def _initiate(self) -> None:
        self._dataset_name = self.zip_archive_path.stem
        self._data_type = self.zip_archive_path.stem.split("_")[1]
        dtype_lower = self._data_type.lower()
        self._data_type_internal = mapper_data_type_to_internal.get(
            dtype_lower, default=dtype_lower
        )

    @property
    def data_type_internal(self) -> str:
        return self._data_type_internal

    @property
    def data_type(self) -> str:
        return self._data_type

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
    def zip_archive_path(self) -> pathlib.Path:
        return self._zip_archive_path

    @property
    def unzipped_archive_directory(self) -> pathlib.Path:
        return self._unzipped_archive_directory

    @property
    def shark_data_path(self) -> pathlib.Path:
        return self.unzipped_archive_directory / "shark_data.txt"

    @property
    def received_data_directory(self) -> pathlib.Path:
        return self.unzipped_archive_directory / "received_data"

    @property
    def processed_data_directory(self) -> pathlib.Path:
        return self.unzipped_archive_directory / "processed_data"

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
        return str(min(self.data["sample_reported_longitude"].astype(float)))

    @property
    def max_longitude(self) -> str:
        return str(max(self.data["sample_reported_longitude"].astype(float)))

    @property
    def min_latitude(self) -> str:
        return str(min(self.data["sample_reported_latitude"].astype(float)))

    @property
    def max_latitude(self) -> str:
        return str(max(self.data["sample_reported_latitude"].astype(float)))

    def _unzip_archive(self):
        self._unzipped_archive_directory = utils.unzip_file(
            self._zip_archive_path, utils.get_temp_directory("zip"), delete_old=True
        )

    def _load_delivery_note(self) -> None:
        self._delivery_note = delivery_note.DeliveryNote.from_txt_file(
            self.delivery_note_path
        )

    def _load_sampling_info(self) -> None:
        if not self.sampling_info_path.exists():
            adm_logger.log_workflow(
                f"No sampling info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._sampling_info = sampling_info.SamplingInfo.from_txt_file(
            self.sampling_info_path
        )

    def _load_analyse_info(self) -> None:
        if not self.analyse_info_path.exists():
            adm_logger.log_workflow(
                f"No analyse info file for {self.dataset_name}", level=adm_logger.INFO
            )
            return
        self._analyse_info = analyse_info.AnalyseInfo.from_txt_file(
            self.analyse_info_path
        )

    # def _add_concatenated_column(
    #     self, new_column: str, columns_to_use: list[str]
    # ) -> None:
    #     """Adds a concatenated column specified in new_column using the columns listed
    #     in columns_to_use"""
    #     for col in columns_to_use:
    #         if new_column not in self._data.columns:
    #             self._data[new_column] = self._data[col]
    #         else:
    #             self._data[new_column] = self._data[new_column] + self._data[col]

    def _check_data_source(self, data_source: data_source.DataFile) -> None:
        if data_source.data_type.lower() != self.data_type.lower():
            msg = f"Data source {data_source} is not of type {self.data_type}"
            logger.error(msg)
            raise ValueError(msg)

    @staticmethod
    def _get_data_from_data_source(data_source: data_source.DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna("")
        data.reset_index(inplace=True, drop=True)
        return data

    def _set_data_source(self, data_source: data_source.DataFile) -> None:
        """Sets a single data source to self._data"""
        self._add_data_source(data_source)
        self._data = self._get_data_from_data_source(data_source)

    def _add_data_source(self, data_source: data_source.DataFile) -> None:
        """Adds a data source to instance variable self._data_sources.
        This method is not adding to data itself."""
        # self._check_data_source(data_source)
        self._data_sources[str(data_source)] = data_source

    def _load_data(self) -> None:
        if not self.shark_data_path.exists():
            logger.error(
                f"Could not find any data file in delivery: {self.shark_data_path}"
            )
            return
        d_source = data_source.TxtRowFormatDataFile(
            path=self.shark_data_path, data_type=self.delivery_note.data_type
        )
        self._set_data_source(d_source)

    def remove_processed_data_directory(self) -> None:
        directory = self.unzipped_archive_directory / "processed_data"
        self._remove_directory(directory)

    def remove_received_data_directory(self) -> None:
        directory = self.unzipped_archive_directory / "received_data"
        self._remove_directory(directory)

    def remove_readme_files(self) -> None:
        for path in self.unzipped_archive_directory.iterdir():
            if "readme" in path.name.lower():
                os.remove(path)

    def list_files(self) -> list[pathlib.Path]:
        paths = []
        for root, dirs, files in os.walk(self.unzipped_archive_directory):
            for file in files:
                path = pathlib.Path(root, file).relative_to(
                    self.unzipped_archive_directory.parent
                )
                paths.append(path)
        return paths

    def _remove_directory(self, directory: pathlib.Path) -> None:
        if not directory.exists():
            return
        adm_logger.log_workflow(
            f"Removing directory: "
            f"{directory.relative_to(self.unzipped_archive_directory.parent)}",
            level=adm_logger.WARNING,
        )
        shutil.rmtree(directory)

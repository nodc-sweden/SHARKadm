import datetime
import logging
from abc import ABC, abstractmethod

import pandas as pd
import polars as pl

from sharkadm import config
from sharkadm.data.data_source.base import (
    DataFile,
    DataSource,
    PolarsDataFile,
    PolarsDataSource,
)
from sharkadm.sharkadm_logger import adm_logger

logger = logging.getLogger(__name__)


class DataHolder(ABC):
    """Class to hold data from a specific data type. Add data using the
    add_data_source method"""

    def __init__(self, *args, **kwargs):
        self._data_sources: dict[str, DataSource | PolarsDataSource] = dict()
        self._number_metadata_rows = 0
        self._header_mapper = None
        self._qf_column_prefix = None
        self._data_structure = "row"

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__} (data type = "{self.data_type}"): '
            f"{self.dataset_name}"
        )

    @abstractmethod
    def __add__(self, other): ...

    @property
    def workflow_message(self) -> str:
        return f"Using DataHolder: {self.__class__.__name__}"

    @property
    def data_holder_name(self) -> str:
        """Short name of the data_holder"""
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_data_holder_description() -> str:
        """Verbal description describing what the data_holder is doing"""
        ...

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, df: pd.DataFrame | pl.DataFrame) -> None:
        if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
            raise "Data must be of type pd.DataFrame"
        self._data = df

    @property
    def data_structure(self) -> str:
        return self._data_structure

    @data_structure.setter
    def data_structure(self, data_structure):
        data_structure_lower = data_structure.lower()
        if data_structure_lower not in config.DATA_STRUCTURES:
            raise ValueError(f"Invalid data structure: {data_structure}")
        self._data_structure = data_structure_lower

    @property
    @abstractmethod
    def data_type(self) -> str: ...

    @property
    @abstractmethod
    def data_type_internal(self) -> str: ...

    @property
    @abstractmethod
    def dataset_name(self) -> str: ...

    @property
    def number_metadata_rows(self) -> int:
        return self._number_metadata_rows

    @property
    @abstractmethod
    def columns(self) -> list[str]: ...

    @property
    def mapped_columns(self) -> dict[str, str]:
        mapped = dict()
        for name, source in self._data_sources.items():
            mapped.update(source.mapped_columns)
        return mapped

    @property
    def not_mapped_columns(self) -> list[str]:
        not_mapped = set()
        for name, source in self._data_sources.items():
            not_mapped.update(source.not_mapped_columns)
        return list(not_mapped)

    @property
    def header_mapper(self):
        mappers = []
        for source in self._data_sources.values():
            if not source.header_mapper:
                continue
            mappers.append(source.header_mapper)
        if len(mappers) != 1:
            return None
        return mappers[0]

    @property
    def qf_column_prefixes(self) -> list[str]:
        prefixes = ["QFLAG."]
        if self._qf_column_prefix:
            prefixes.append(self._qf_column_prefix)
        return prefixes

    @property
    def original_qf_column_prefix(self) -> str | None:
        return self._qf_column_prefix

    @property
    @abstractmethod
    def year_span(self) -> list[str]: ...

    @property
    def zip_archive_base(self) -> str:
        if not self._data_sources:
            return self._get_created_zip_archive_base()
        source = next(iter(self._data_sources.values()))
        if not hasattr(source, "path"):
            return self._get_created_zip_archive_base()
        if not source.path:
            return self._get_created_zip_archive_base()
        parts = source.path.parts
        if "processed_data" not in parts:
            return self._get_created_zip_archive_base()
        return parts[parts.index("processed_data") - 1]

    def _get_created_zip_archive_base(self) -> str:
        parts = ["SHARK", self.data_type.replace(" ", "")]
        years = self.year_span
        parts.append(years[0])
        if years[0] != years[1]:
            parts.append(years[1])
        if hasattr(self, "delivery_note"):
            parts.append(self.delivery_note.reporting_institute_code)
        return "_".join(parts)

    @property
    def zip_archive_name(self) -> str:
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        return f"{self.zip_archive_base}_version_{today}"

    def get_original_name(self, internal_name: str):
        return self.header_mapper.get_external_name(internal_name)

    def _add_data_source(self, data_source: DataSource | PolarsDataSource) -> None:
        """Adds a data source to instance variable self._data_sources.
        This method is not adding to data itself."""
        self._check_data_source(data_source)
        self._data_sources[str(data_source)] = data_source

    def _check_data_source(self, data_source: DataSource | PolarsDataSource) -> None:
        # Can be overwritten in child classes
        return


class PandasDataHolder(DataHolder, ABC):
    def __init__(self, *args, **kwargs):
        self._data = pd.DataFrame()
        super().__init__(*args, **kwargs)

    def __add__(self, other):
        if self.data_type != other.data_type:
            adm_logger.log_workflow(
                f"Not allowed to merge data_sources of different data_types: "
                f"{self.data_type} and {other.data_type}"
            )
            return
        if self.dataset_name == other.dataset_name:
            adm_logger.log_workflow(
                f"Not allowed to merge to instances of the same dataset: "
                f"{self.dataset_name}"
            )
            return
        concat_data = pd.concat([self.data, other.data], axis=0)
        concat_data.fillna("", inplace=True)
        concat_data.reset_index(inplace=True)
        cdh = ConcatDataHolder()
        cdh.data = concat_data
        cdh.data_type = self.data_type
        return cdh

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, df: pd.DataFrame) -> None:
        if type(df) is not pd.DataFrame:
            raise TypeError("Data must be of type pandas.DataFrame")
        self._data = df

    def columns(self) -> list[str]:
        return sorted(self.data.columns)

    def year_span(self) -> list[str]:
        years = list(set(self.data["visit_year"]))
        if len(years) == 1:
            return [years[0], years[0]]
        sorted_years = sorted(years)
        return [sorted_years[0], sorted_years[-1]]

    @staticmethod
    def _get_data_from_data_source(data_source: DataSource) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna("")
        data.reset_index(inplace=True, drop=True)
        return data

    def _set_data_source(self, data_source: DataSource) -> None:
        """Sets a single data source to self._data"""
        self._add_data_source(data_source)
        self._data = self._get_data_from_data_source(data_source)


class PolarsDataHolder(DataHolder, ABC):
    def __init__(self, *args, **kwargs):
        self._data = pl.DataFrame()
        super().__init__(*args, **kwargs)

    def __add__(self, other):
        if self.data_type != other.data_type:
            adm_logger.log_workflow(
                f"Not allowed to merge data_sources of different data_types: "
                f"{self.data_type} and {other.data_type}"
            )
            return
        if self.dataset_name == other.dataset_name:
            adm_logger.log_workflow(
                f"Not allowed to merge to instances of the same dataset: "
                f"{self.dataset_name}"
            )
            return
        concat_data = pl.concat([self.data, other.data])
        cdh = ConcatDataHolder()
        cdh.data = concat_data
        cdh.data_type = self.data_type
        return cdh

    @property
    def data(self) -> pl.DataFrame:
        return self._data

    @data.setter
    def data(self, df: pl.DataFrame) -> None:
        if type(df) not in [pl.DataFrame, pd.DataFrame]:
            raise TypeError(
                "Data must be of type polars.DataFrame or pandas.DataFrame "
                f"(was '{type(df)}')"
            )
        self._data = df

    def columns(self) -> list[str]:
        return sorted(self.data.columns)

    def year_span(self) -> list[str]:
        years = list(set(self.data["visit_year"]))
        if len(years) == 1:
            return [years[0], years[0]]
        sorted_years = sorted(years)
        return [sorted_years[0], sorted_years[-1]]

    @staticmethod
    def _get_data_from_data_source(data_source: PolarsDataFile) -> pl.DataFrame:
        data = data_source.get_data()
        data = data.fill_nan("")
        return data

    def _set_data_source(self, data_source: PolarsDataFile) -> None:
        """Sets a single data source to self._data"""
        self._add_data_source(data_source)
        self._data = self._get_data_from_data_source(data_source)


class old_DataHolder(ABC):
    """Class to hold data from a specific data type.
    Add data using the add_data_source method"""

    def __init__(self, *args, **kwargs):
        self._data = pd.DataFrame()
        self._data_sources: dict[str, DataFile] = dict()
        self._number_metadata_rows = 0
        self._header_mapper = None
        self._qf_column_prefix = None
        self._data_structure = "row"

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__} (data type = "{self.data_type}"): '
            f"{self.dataset_name}"
        )

    def __add__(self, other):
        if self.data_type != other.data_type:
            adm_logger.log_workflow(
                f"Not allowed to merge data_sources of different data_types: "
                f"{self.data_type} and {other.data_type}"
            )
            return
        if self.dataset_name == other.dataset_name:
            adm_logger.log_workflow(
                f"Not allowed to merge to instances of the same dataset: "
                f"{self.dataset_name}"
            )
            return
        concat_data = pd.concat([self.data, other.data], axis=0)
        concat_data.fillna("", inplace=True)
        concat_data.reset_index(inplace=True)
        cdh = ConcatDataHolder()
        cdh.data = concat_data
        cdh.data_type = self.data_type
        return cdh

    @property
    def workflow_message(self) -> str:
        return f"Using DataHolder: {self.__class__.__name__}"

    @property
    def data_holder_name(self) -> str:
        """Short name of the data_holder"""
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def get_data_holder_description() -> str:
        """Verbal description describing what the data_holder is doing"""
        ...

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, df: pd.DataFrame) -> None:
        if not isinstance(df, pd.DataFrame):
            raise "Data must be of type pd.DataFrame"
        self._data = df

    @property
    def data_structure(self) -> str:
        return self._data_structure

    @data_structure.setter
    def data_structure(self, data_structure):
        data_structure_lower = data_structure.lower()
        if data_structure_lower not in config.DATA_STRUCTURES:
            raise ValueError(f"Invalid data structure: {data_structure}")
        self._data_structure = data_structure_lower

    # @property
    # @abstractmethod
    # def data(self) -> pd.DataFrame:
    #     ...

    @property
    @abstractmethod
    def data_type(self) -> str: ...

    @property
    @abstractmethod
    def dataset_name(self) -> str: ...

    @property
    def number_metadata_rows(self) -> int:
        return self._number_metadata_rows

    @property
    def columns(self) -> list[str]:
        return sorted(self.data.columns)

    @property
    def mapped_columns(self) -> dict[str, str]:
        mapped = dict()
        for name, source in self._data_sources.items():
            mapped.update(source.mapped_columns)
        return mapped

    @property
    def not_mapped_columns(self) -> list[str]:
        not_mapped = set()
        for name, source in self._data_sources.items():
            not_mapped.update(source.not_mapped_columns)
        return list(not_mapped)

    @property
    def header_mapper(self):
        mappers = []
        for source in self._data_sources.values():
            if not source.header_mapper:
                continue
            mappers.append(source.header_mapper)
        if len(mappers) != 1:
            return None
        return mappers[0]

    @property
    def qf_column_prefixes(self) -> list[str]:
        prefixes = ["QFLAG."]
        if self._qf_column_prefix:
            prefixes.append(self._qf_column_prefix)
        return prefixes

    @property
    def original_qf_column_prefix(self) -> str | None:
        return self._qf_column_prefix

    @property
    def year_span(self) -> list[str]:
        years = list(set(self.data["visit_year"]))
        if len(years) == 1:
            return [years[0], years[0]]
        sorted_years = sorted(years)
        return [sorted_years[0], sorted_years[-1]]

    @property
    def zip_archive_base(self) -> str:
        parts = ["SHARK", self.data_type.capitalize()]
        years = self.year_span
        parts.append(years[0])
        if years[0] != years[1]:
            parts.append(years[1])
        if hasattr(self, "delivery_note"):
            parts.append(self.delivery_note.reporting_institute_code)
        return "_".join(parts)

    @property
    def zip_archive_name(self) -> str:
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        return f"{self.zip_archive_base}_version_{today}"

    def get_original_name(self, internal_name: str):
        return self.header_mapper.get_external_name(internal_name)

    @staticmethod
    def _get_data_from_data_source(data_source: DataFile) -> pd.DataFrame:
        data = data_source.get_data()
        data = data.fillna("")
        data.reset_index(inplace=True, drop=True)
        return data

    def _set_data_source(self, data_source: DataFile) -> None:
        """Sets a single data source to self._data"""
        self._add_data_source(data_source)
        self._data = self._get_data_from_data_source(data_source)

    def _add_data_source(self, data_source: DataFile) -> None:
        """Adds a data source to instance variable self._data_sources.
        This method is not adding to data itself."""
        self._check_data_source(data_source)
        self._data_sources[str(data_source)] = data_source

    def _check_data_source(self, data_source: DataFile) -> None:
        # Can be overwritten in child classes
        return


class ConcatDataHolder(PandasDataHolder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._data_type = ""

    @staticmethod
    def get_data_holder_description() -> str:
        return "This is a concatenated data holder"

    @property
    def data_type(self) -> str:
        return self._data_type

    @data_type.setter
    def data_type(self, data_type: str) -> None:
        self._data_type = data_type

    @property
    def dataset_name(self) -> str:
        return "#".join(self.data["dataset_name"].unique())

    @property
    def number_metadata_rows(self) -> None:
        return

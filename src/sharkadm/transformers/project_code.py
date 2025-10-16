import polars as pl

from .. import adm_logger
from ..data.zip_archive import PolarsZipArchiveDataHolder
from . import PolarsTransformer
from ._codes import _AddCodesProj, _PolarsAddCodesProj


class AddSwedishProjectName(_AddCodesProj):
    source_cols = ("sample_project_name", "sample_project_name_en")
    col_to_set = "sample_project_name_sv"
    lookup_key = "swedish_name"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds project name in swedish"


class AddEnglishProjectName(_AddCodesProj):
    source_cols = ("sample_project_name", "sample_project_name_sv")
    col_to_set = "sample_project_name_en"
    lookup_key = "english_name"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds project name in english"


class PolarsAddSwedishProjectName(_PolarsAddCodesProj):
    source_cols = ("sample_project_name", "sample_project_name_en")
    col_to_set = "sample_project_name_sv"
    lookup_key = "swedish_name"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds project name in swedish"


class PolarsAddEnglishProjectName(_PolarsAddCodesProj):
    source_cols = ("sample_project_name", "sample_project_name_sv")
    col_to_set = "sample_project_name_en"
    lookup_key = "english_name"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds project name in english"


class PolarsAddProjectCodeFromMetadata(PolarsTransformer):
    valid_data_holders = ("ZipArchiveDataHolder",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds project codes from metadata.txt"

    def _transform(self, data_holder: PolarsZipArchiveDataHolder) -> None:
        boolean = pl.Series([True] * len(data_holder.data))
        lookup_col = "sample_project_name_en"
        boolean = boolean & ((pl.col(lookup_col) == "-") | (pl.col(lookup_col) == ""))
        for (statn, date, _id), df in data_holder.data.filter(boolean).group_by(
            "reported_station_name", "visit_date", "visit_id"
        ):
            info = data_holder.metadata.get_info(
                reported_station_name=statn,
                visit_date=date,
                visit_id=_id,
            )
            if not info:
                self._log(
                    "No match in metadata to add project!", level=adm_logger.WARNING
                )
                return
            if len(info) > 1:
                self._log(
                    f"To many matches {len(info)} in metadata to add project",
                    level=adm_logger.WARNING,
                )
                return
            visit_boolean = (
                boolean
                & (pl.col("reported_station_name") == statn)
                & (pl.col("visit_date") == date)
                & (pl.col("visit_id") == _id)
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(visit_boolean).then(info["sample_project_name"])
            )

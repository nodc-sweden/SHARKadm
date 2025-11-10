import polars as pl

from .. import adm_logger
from ..data.zip_archive import PolarsZipArchiveDataHolder
from . import PolarsTransformer


class PolarsAddFromMetadata(PolarsTransformer):
    valid_data_holders = ("PolarsZipArchiveDataHolder",)
    valid_data_types = ("profile",)

    columns: tuple[str] = ()

    def __init__(self, *columns: str):
        super().__init__()
        self._columns = columns or self.columns

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds info from metadata.txt for the given columns"

    def _transform(self, data_holder: PolarsZipArchiveDataHolder) -> None:
        for col in self._columns:
            self._add_empty_col(data_holder, col)

        for (statn, date, _id), df in data_holder.data.group_by(
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
                (pl.col("reported_station_name") == statn)
                & (pl.col("visit_date") == date)
                & (pl.col("visit_id") == _id)
            )
            for col in self._columns:
                data_holder.data = data_holder.data.with_columns(
                    pl.when(visit_boolean)
                    .then(pl.lit(info[0][col]))
                    .otherwise(pl.col(col))
                    .alias(col)
                )

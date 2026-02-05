import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class PolarsSetReportedAphiaIdFromAphiaId(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "aphia_id"
    col_to_set = "reported_aphia_id"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsSetReportedAphiaIdFromAphiaId.col_to_set} "
            f"from {PolarsSetReportedAphiaIdFromAphiaId.source_col} if not given."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_set in data_holder.data.columns:
            self._log(
                f"Column {self.col_to_set} already in data. Will not add",
                level=adm_logger.DEBUG,
            )
            return
        if self.source_col not in data_holder.data.columns:
            self._log(
                f"No source column {self.source_col}. "
                f"Setting empty column {self.col_to_set}",
                level=adm_logger.DEBUG,
            )
            self._add_empty_col_to_set(data_holder)
            return
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )


class PolarsSetAphiaIdFromReportedAphiaId(PolarsTransformer):
    valid_data_types = ("plankton_imaging",)
    source_col = "reported_aphia_id"
    col_to_set = "aphia_id"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsSetAphiaIdFromReportedAphiaId.col_to_set} "
            f"from {PolarsSetAphiaIdFromReportedAphiaId.source_col} if it is a digit."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )
        self._log(
            f"Setting {self.col_to_set} from {self.source_col}", level=adm_logger.DEBUG
        )


class PolarsSetAphiaIdFromBvolAphiaId(PolarsTransformer):
    valid_data_types = ("plankton_imaging", "phytoplankton")
    source_col = "bvol_aphia_id"
    col_to_set = "aphia_id"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsSetAphiaIdFromBvolAphiaId.col_to_set} "
            f"from {PolarsSetAphiaIdFromBvolAphiaId.source_col} if it is a digit."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.source_col not in data_holder.data.columns:
            self._log(
                f"Column {self.source_col} not found. Will not add",
                level=adm_logger.DEBUG,
            )
            return
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )
        self._log(
            f"Setting {self.col_to_set} from {self.source_col}", level=adm_logger.DEBUG
        )


class PolarsSetAphiaIdFromWormsAphiaId(PolarsTransformer):
    valid_data_types = ("plankton_imaging", "phytoplankton")
    source_col = "worms_aphia_id"
    col_to_set = "aphia_id"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsSetAphiaIdFromWormsAphiaId.col_to_set} "
            f"from {PolarsSetAphiaIdFromWormsAphiaId.source_col} if it is a digit."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.source_col not in data_holder.data.columns:
            self._log(
                f"Column {self.source_col} not found. Will not add",
                level=adm_logger.DEBUG,
            )
            return
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )
        self._log(
            f"Setting {self.col_to_set} from {self.source_col}", level=adm_logger.DEBUG
        )

import numpy as np
import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import DataHolderProtocol, PolarsTransformer, Transformer


class ManualSealPathology(Transformer):
    valid_data_types = ("SealPathology",)
    valid_data_holders = ("ZipArchiveDataHolder",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Manual fixes for SealPathology"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        md5 = "364768f88de5f22c0e415150eddee722"
        boolean = data_holder.data["shark_sample_id_md5"] == md5
        df = data_holder.data[boolean]
        if df.empty:
            self._log(f"md5 not found: {md5}", level=adm_logger.INFO)
            return
        data_holder.data.loc[boolean, "visit_year"] = "2018"
        data_holder.data.loc[boolean, "visit_month"] = "01"
        data_holder.data.loc[boolean, "sample_date"] = "2018-01-01"


class ManualHarbourPorpoise(Transformer):
    valid_data_types = ("HarbourPorpoise",)
    valid_data_holders = ("ZipArchiveDataHolder",)
    source_col = "observation_date"
    col_to_set = "visit_date"

    @staticmethod
    def get_transformer_description() -> str:
        return "Manual fixes for HarbourPorpoise"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Source column {self.source_col} not found", level=adm_logger.DEBUG
            )
            return
        md5s = [
            "d15bc86c3b84b65c227da85d48db5091",
            "6f080a561c6e18b4ec3f436ea84cc33d",
            "549c1e46578ff95d694cfb21139ffc67",
            "777f6330cccafaa4de98bbebba2fa76b",
            "634b1bc7ae5b70cf65bb1699acfd8b2e",
            "f96e51d95f867a316ed09b2724e2e9d2",
            "6fd78ab27999e2b08a2d6cdb494dc14c",
            "664ee45cb5fe541867ea00edf9b83ba6",
        ]
        for md5 in md5s:
            boolean = data_holder.data["shark_sample_id_md5"] == md5
            df = data_holder.data[boolean]
            if df.empty:
                self._log(f"md5 not found: {md5}", level=adm_logger.DEBUG)
                continue
            data_holder.data.loc[boolean, self.col_to_set] = data_holder.data.loc[
                boolean, self.source_col
            ]
            self._log(
                f"{self.col_to_set} set from {self.source_col} for md5={md5} "
                f"in {len(np.where(boolean)[0])} places",
                level=adm_logger.INFO,
            )


class PolarsManualSealPathology(PolarsTransformer):
    valid_data_types = ("SealPathology",)
    valid_data_holders = ("PolarsZipArchiveDataHolder",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Manual fixes for SealPathology"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        md5 = "364768f88de5f22c0e415150eddee722"
        if md5 not in data_holder.data["shark_sample_id_md5"]:
            return
        self._log(f"Setting manual info for md5: {md5}", level=adm_logger.INFO)
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col("shark_sample_id_md5") == md5)
            .then(pl.lit("2018"))
            .otherwise(pl.col("visit_year"))
            .alias("visit_year"),
            pl.when(pl.col("shark_sample_id_md5") == md5)
            .then(pl.lit("01"))
            .otherwise(pl.col("visit_month"))
            .alias("visit_month"),
            pl.when(pl.col("shark_sample_id_md5") == md5)
            .then(pl.lit("2018-01-01"))
            .otherwise(pl.col("sample_date"))
            .alias("sample_date"),
        )


class PolarsManualHarbourPorpoise(PolarsTransformer):
    valid_data_types = ("HarbourPorpoise",)
    valid_data_holders = ("PolarsZipArchiveDataHolder",)
    md5_column = "shark_sample_id_md5"
    source_col = "observation_date"
    col_to_set = "visit_date"

    @staticmethod
    def get_transformer_description() -> str:
        return "Manual fixes for HarbourPorpoise"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Source column {self.source_col} not found", level=adm_logger.DEBUG
            )
            return
        md5s = [
            "d15bc86c3b84b65c227da85d48db5091",
            "6f080a561c6e18b4ec3f436ea84cc33d",
            "549c1e46578ff95d694cfb21139ffc67",
            "777f6330cccafaa4de98bbebba2fa76b",
            "634b1bc7ae5b70cf65bb1699acfd8b2e",
            "f96e51d95f867a316ed09b2724e2e9d2",
            "6fd78ab27999e2b08a2d6cdb494dc14c",
            "664ee45cb5fe541867ea00edf9b83ba6",
        ]
        for md5 in md5s:
            if md5 not in data_holder.data[self.md5_column]:
                continue
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.md5_column) == md5)
                .then(pl.col(self.source_col))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )
            self._log(
                f"{self.col_to_set} set from {self.source_col} for md5={md5}",
                level=adm_logger.INFO,
            )

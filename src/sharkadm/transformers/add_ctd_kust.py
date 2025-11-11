from typing import ClassVar, Dict

import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import PolarsTransformer


class AddCtdKust(PolarsTransformer):
    #    valid_data_types = ("physicalchemical",)
    col_mapper: ClassVar[Dict[str, str]] = {
        "TEMP_CTD_KUST": "COPY_VARIABLE.Temperature CTD.C",
        "SALT_CTD_KUST": "COPY_VARIABLE.Salinity CTD.o/oo psu",
        "CNDC_CTD_KUST": "COPY_VARIABLE.Conductivity CTD.mS/m",
        "DOXY_CTD_KUST": "COPY_VARIABLE.Dissolved oxygen O2 CTD.ml/l",
    }

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds CTD_KUST if existing to CTD, if the latter is missing"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        for kust_col, ctd_col in self.col_mapper.items():
            if kust_col in data_holder.data.columns:
                if (
                    data_holder.data[ctd_col] == ""
                ).sum() == data_holder.data.height and (
                    data_holder.data[kust_col] != ""
                ).sum() > 0:
                    adm_logger.log_transformation(
                        f"Replacing empty '{ctd_col}' with '{kust_col}'",
                        level=adm_logger.INFO,
                    )
                    data_holder.data = data_holder.data.with_columns(
                        pl.col(kust_col).alias(ctd_col)
                    )

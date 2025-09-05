import numpy as np
import polars as pl
from gsw.conversions import p_from_z

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import PolarsTransformer


def pressure(depth, latitude):
    return p_from_z(-depth, latitude)


class PolarsAddPressure(PolarsTransformer):
    valid_data_structures = ("column", "row")
    depth_col = "sample_depth_m"
    latitude_col = "sample_latitude_dd"
    col_to_set = "pressure"  # in dbar

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating pressure. "
            f"Setting value to column {PolarsAddPressure.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_set in data_holder.data.columns:
            adm_logger.log_transformation(
                f"{self.col_to_set} already exists, values will be recalculated",
                level=adm_logger.INFO,
            )
        must_haves = [
            self.latitude_col,
            self.depth_col,
        ]
        if not all(col in data_holder.data.columns for col in must_haves):
            adm_logger.log_transformation(
                "Missing required columns: "
                f"{[col for col in must_haves if col not in data_holder.data.columns]}",
                level=adm_logger.WARNING,
            )
            return

        valid_rows = (
            data_holder.data[self.latitude_col].cast(pl.Float64).is_not_null()
            & data_holder.data[self.depth_col].cast(pl.Float64).is_not_null()
        )

        if valid_rows.any():
            valid_data = data_holder.data.filter(valid_rows)
            depth = valid_data[self.depth_col].cast(pl.Float64).to_numpy()
            lat = valid_data[self.latitude_col].cast(pl.Float64).to_numpy()

            pres = np.full(len(data_holder.data), np.nan)
            pres[valid_rows.to_numpy()] = pressure(depth, lat)

            data_holder.data = data_holder.data.with_columns(
                pl.when(valid_rows)
                .then(
                    pl.Series(pres).map_elements(
                        lambda x: None if np.isnan(x) else x, return_dtype=pl.Float64
                    )
                )
                .otherwise(None)
                .alias(self.col_to_set)
            )
        else:
            adm_logger.log_transformation(
                "No valid data rows for pressure calculation.", level=adm_logger.WARNING
            )

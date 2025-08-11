import numpy as np
import polars as pl
from gsw import CT_from_pt, SA_from_SP, pt_from_t, rho
from gsw.conversions import p_from_z

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import PolarsTransformer


def pressure(depth, latitude):
    return p_from_z(-depth, latitude)


def in_situ_density(
    practical_salinity, temperature, depth, p_reference, longitude, latitude
):
    p = pressure(depth, latitude)
    absolute_salinity = SA_from_SP(practical_salinity, p, longitude, latitude)
    potential_temperature = pt_from_t(absolute_salinity, temperature, p, p_reference)
    conservative_temperature = CT_from_pt(absolute_salinity, potential_temperature)
    return rho(absolute_salinity, conservative_temperature, p)


class PolarsAddDensityWide(PolarsTransformer):
    valid_data_structures = ("column",)
    practical_salinity = "Salinity CTD"
    temperature = "Temperature CTD"
    depth_col = "sample_depth_m"
    longitude_col = "sample_latitude_dd"
    latitude_col = "sample_longitude_dd"
    col_to_set = "in_situ_density"  # in kg/m3
    p_reference = 0  # reference pressure for pot. temperature set to 0 dbar.

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating density. "
            f"Setting value to column {PolarsAddDensityWide.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_set in data_holder.data.columns:
            adm_logger.info(
                f"{self.col_to_set} already exists, values will be recalculated"
            )
        required_cols = [
            self.longitude_col,
            self.latitude_col,
            self.depth_col,
            self.practical_salinity,
            self.temperature,
        ]
        if not all(col in data_holder.data.columns for col in required_cols):
            adm_logger.warning(
                "Missing required columns: "
                f"{[col for col in required_cols if col not in data_holder.data.columns]}"
            )
            return

        valid_rows = (
            data_holder.data[self.longitude_col].cast(pl.Float64).is_not_null()
            & data_holder.data[self.latitude_col].cast(pl.Float64).is_not_null()
            & data_holder.data[self.depth_col].cast(pl.Float64).is_not_null()
            & data_holder.data[self.practical_salinity].cast(pl.Float64).is_not_null()
            & data_holder.data[self.temperature].cast(pl.Float64).is_not_null()
        )

        if valid_rows.any():
            valid_data = data_holder.data.filter(valid_rows)
            psu = valid_data[self.practical_salinity].cast(pl.Float64).to_numpy()
            temp = valid_data[self.temperature].cast(pl.Float64).to_numpy()
            depth = valid_data[self.depth_col].cast(pl.Float64).to_numpy()
            lon = valid_data[self.longitude_col].cast(pl.Float64).to_numpy()
            lat = valid_data[self.latitude_col].cast(pl.Float64).to_numpy()

            dens = np.full(len(data_holder.data), np.nan)
            dens[valid_rows.to_numpy()] = in_situ_density(
                psu, temp, depth, self.p_reference, lon, lat
            )

            data_holder.data = data_holder.data.with_columns(
                pl.when(valid_rows)
                .then(pl.Series(dens))
                .otherwise(None)
                .alias(self.col_to_set)
            )

        else:
            adm_logger.warning("No valid data rows for density calculation.")


class PolarsAddDensity(PolarsTransformer):
    valid_data_structures = ("row",)
    practical_salinity = "Salinity CTD"
    temperature = "Temperature CTD"
    depth_col = "sample_depth_m"
    longitude_col = "sample_latitude_dd"
    latitude_col = "sample_longitude_dd"
    col_to_set = "in_situ_density"  # in kg/m3
    p_reference = 0  # reference pressure for pot. temperature set to 0 dbar.

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating density. Setting value to column {PolarsAddDensity.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_set in data_holder.data.columns:
            adm_logger.info(
                f"{self.col_to_set} already exists, values will be recalculated"
            )
        required_cols = [
            "visit_key",
            self.longitude_col,
            self.latitude_col,
            self.depth_col,
        ]
        if not all(col in data_holder.data.columns for col in required_cols):
            adm_logger.warning(
                "Missing required columns: "
                f"{[col for col in required_cols if col not in data_holder.data.columns]}"
            )
            return
        elif (
            data_holder.data["parameter"]
            .is_in([self.practical_salinity, self.temperature])
            .sum()
            < 2
        ):
            adm_logger.warning(
                f"Missing required {self.practical_salinity} and/or {self.temperature}."
            )
            return
        elif (
            data_holder.data["parameter"].is_in(
                [self.practical_salinity, self.temperature]
            )
            & (data_holder.data["value"].is_not_null())
        ).sum() == 0:
            adm_logger.warning(
                f"Not enough {self.practical_salinity} and "
                f"{self.temperature} data to calculate from."
            )
            return

        df = (
            data_holder.data.filter(
                pl.col("parameter").is_in([self.practical_salinity, self.temperature])
                & pl.col("value").is_not_null()
                & pl.col(self.longitude_col).is_not_null()
                & pl.col(self.latitude_col).is_not_null()
                & pl.col(self.depth_col).is_not_null()
            )
            .select(
                [
                    "visit_key",
                    self.depth_col,
                    self.longitude_col,
                    self.latitude_col,
                    "parameter",
                    "value",
                ]
            )
            .pivot(
                values="value",
                index=[
                    "visit_key",
                    self.depth_col,
                    self.longitude_col,
                    self.latitude_col,
                ],
                on="parameter",
                aggregate_function="first",
            )
        )

        if (
            df[self.practical_salinity].is_not_null() & df[self.temperature].is_not_null()
        ).sum() == 0:
            adm_logger.warning(
                f"Not enough {self.practical_salinity} and "
                f"{self.temperature} data to calculate from."
            )
            return

        practical_salinity = df[self.practical_salinity].to_numpy().astype(float)
        temperature = df[self.temperature].to_numpy().astype(float)
        depth = df[self.depth_col].to_numpy().astype(float)
        longitude = df[self.longitude_col].to_numpy().astype(float)
        latitude = df[self.latitude_col].to_numpy().astype(float)

        density_values = in_situ_density(
            practical_salinity, temperature, depth, self.p_reference, longitude, latitude
        )
        df = df.with_columns(pl.Series(self.col_to_set, density_values).fill_nan(None))

        data_holder.data = data_holder.data.join(
            df.select(
                [
                    "visit_key",
                    self.depth_col,
                    self.col_to_set,  # in_situ_density
                ]
            ),
            on=["visit_key", self.depth_col],
            how="left",
        )

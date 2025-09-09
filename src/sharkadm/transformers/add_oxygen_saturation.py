from typing import ClassVar

import polars as pl
from gsw import O2sol_SP_pt, SA_from_SP, pt_from_t
from gsw.conversions import p_from_z

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import PolarsTransformer


def pressure(depth, latitude):
    return p_from_z(-depth, latitude)


def oxygen_saturation(
    practical_salinity, temperature, depth, p_reference, longitude, latitude
):
    p = pressure(depth, latitude)
    absolute_salinity = SA_from_SP(practical_salinity, p, longitude, latitude)
    potential_temperature = pt_from_t(absolute_salinity, temperature, p, p_reference)
    return O2sol_SP_pt(practical_salinity, potential_temperature)


def oxygen_from_umol_kg_2_ml_l(oxygen, density):
    # Unit conversions from ICES:
    # https://www.ices.dk/data/tools/Pages/Unit-conversions.aspx
    return (oxygen * (density / 1000)) / 44.661


def oxygen_from_ml_l_2_umol_kg(oxygen, density):
    # Unit conversions from ICES:
    # https://www.ices.dk/data/tools/Pages/Unit-conversions.aspx
    return (oxygen * 44.661) / (density / 1000)


class PolarsAddOxygenSaturation(PolarsTransformer):
    valid_data_structures = ("row",)
    practical_salinity = "Salinity CTD"
    temperature = "Temperature CTD"
    oxygen = "Dissolved oxygen O2 CTD"
    depth_col = "sample_depth_m"
    longitude_col = "sample_longitude_dd"
    latitude_col = "sample_latitude_dd"
    density_col = "in_situ_density"
    cols_to_set: ClassVar[list[str]] = [
        "oxygen_at_saturation_in_ml_per_l",
        "oxygen_saturation_in_percent",
    ]
    p_reference = 0  # reference pressure for pot. temperature set to 0 dbar.

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating oxygen at saturation in ml/l and "
            f"the oxygen saturation of the water body in %. "
            f"Setting value to column {', '.join(PolarsAddOxygenSaturation.cols_to_set)}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if [col for col in self.cols_to_set if col in data_holder.data.columns]:
            adm_logger.log_transformation(
                "Calculated oxygen parameters already exists, "
                "values will be recalculated",
                level=adm_logger.INFO,
            )
        must_haves = [
            "visit_key",
            self.depth_col,
            self.longitude_col,
            self.latitude_col,
            self.density_col,
        ]
        if not all(col in data_holder.data.columns for col in must_haves):
            adm_logger.log_transformation(
                "Missing required columns: "
                f"{[col for col in must_haves if col not in data_holder.data.columns]}",
                level=adm_logger.WARNING,
            )
            return
        if (
            data_holder.data["parameter"]
            .is_in([self.practical_salinity, self.temperature, self.oxygen])
            .sum()
            < 3
        ):
            adm_logger.log_transformation(
                f"Missing required {self.practical_salinity}, "
                f"{self.temperature} and/or {self.oxygen}.",
                level=adm_logger.WARNING,
            )
            return
        if (
            data_holder.data["parameter"].is_in(
                [self.practical_salinity, self.temperature, self.oxygen]
            )
            & (data_holder.data["value"].is_not_null())
        ).sum() < 3:
            adm_logger.log_transformation(
                f"Not enough {self.practical_salinity} and "
                f"{self.temperature} data to calculate from.",
                level=adm_logger.WARNING,
            )
            return

        df = (
            data_holder.data.filter(
                pl.col("parameter").is_in(
                    [self.practical_salinity, self.temperature, self.oxygen]
                )
                & pl.col("value").is_not_null()
                & pl.col(self.longitude_col).is_not_null()
                & pl.col(self.latitude_col).is_not_null()
                & pl.col(self.depth_col).is_not_null()
                & pl.col(self.density_col).is_not_null()
            )
            .select(
                [
                    "visit_key",
                    self.depth_col,
                    self.longitude_col,
                    self.latitude_col,
                    "parameter",
                    "value",
                    self.density_col,
                ]
            )
            .pivot(
                values="value",
                index=[
                    "visit_key",
                    self.depth_col,
                    self.longitude_col,
                    self.latitude_col,
                    self.density_col,
                ],
                on="parameter",
                aggregate_function="first",
            )
        )

        if (
            df[self.practical_salinity].is_not_null()
            & df[self.temperature].is_not_null()
            & df[self.oxygen].is_not_null()
        ).sum() == 0:
            adm_logger.log_transformation(
                f"Not enough {self.practical_salinity}, "
                f"{self.temperature} and {self.oxygen}  data to calculate from.",
                level=adm_logger.WARNING,
            )
            return

        practical_salinity = df[self.practical_salinity].to_numpy().astype(float)
        temperature = df[self.temperature].to_numpy().astype(float)
        oxygen = df[self.oxygen].to_numpy().astype(float)
        depth = df[self.depth_col].to_numpy().astype(float)
        longitude = df[self.longitude_col].to_numpy().astype(float)
        latitude = df[self.latitude_col].to_numpy().astype(float)
        density = df[self.density_col].to_numpy().astype(float)

        oxygen_values = oxygen_from_umol_kg_2_ml_l(
            oxygen_saturation(
                practical_salinity,
                temperature,
                depth,
                self.p_reference,
                longitude,
                latitude,
            ),
            density,
        )

        oxygen_values_percent = oxygen / oxygen_values * 100

        df = df.with_columns(
            [
                pl.Series(self.cols_to_set[0], oxygen_values).fill_nan(None),
                pl.Series(self.cols_to_set[1], oxygen_values_percent).fill_nan(None),
            ]
        )

        data_holder.data = data_holder.data.join(
            df.select(
                [
                    "visit_key",
                    self.depth_col,
                    *self.cols_to_set,
                ]
            ),
            on=["visit_key", self.depth_col],
            how="left",
        )

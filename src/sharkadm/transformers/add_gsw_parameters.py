from typing import ClassVar

import numpy as np
import polars as pl
from gsw import CT_from_pt, O2sol_SP_pt, SA_from_SP, pt_from_t, rho
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


class PolarsAddPressure(PolarsTransformer):
    valid_data_structures = ("column", "row")
    depth_col = "sample_depth_m"
    latitude_col = "sample_latitude_dd"
    col_to_set = "COPY_VARIABLE.Derived pressure.dbar"

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


class PolarsAddDensityWide(PolarsTransformer):
    valid_data_structures = ("column",)
    practical_salinity = "COPY_VARIABLE.Salinity CTD.o/oo psu"
    temperature = "COPY_VARIABLE.Temperature CTD.C"
    depth_col = "sample_depth_m"
    longitude_col = "sample_longitude_dd"
    latitude_col = "sample_latitude_dd"
    col_to_set = "COPY_VARIABLE.Derived in situ density.kg/m3"  # in kg/m3
    p_reference = 0  # reference pressure for pot. temperature set to 0 dbar.

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating density. "
            f"Setting value to column {PolarsAddDensityWide.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_set in data_holder.data.columns:
            adm_logger.log_transformation(
                f"{self.col_to_set} already exists, values will be recalculated",
                level=adm_logger.INFO,
            )
        must_haves = [
            self.longitude_col,
            self.latitude_col,
            self.depth_col,
            self.practical_salinity,
            self.temperature,
        ]
        if not all(col in data_holder.data.columns for col in must_haves):
            adm_logger.log_transformation(
                "Missing required columns: "
                f"{[col for col in must_haves if col not in data_holder.data.columns]}",
                level=adm_logger.WARNING,
            )
            return

        valid_rows = (
            data_holder.data[self.longitude_col]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.latitude_col]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.depth_col]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.practical_salinity]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.temperature]
            .cast(pl.Float64, strict=False)
            .is_not_null()
        )

        if valid_rows.any():
            valid_data = data_holder.data.filter(valid_rows)
            psal = valid_data[self.practical_salinity].cast(pl.Float64).to_numpy()
            temp = valid_data[self.temperature].cast(pl.Float64).to_numpy()
            depth = valid_data[self.depth_col].cast(pl.Float64).to_numpy()
            lon = valid_data[self.longitude_col].cast(pl.Float64).to_numpy()
            lat = valid_data[self.latitude_col].cast(pl.Float64).to_numpy()

            dens = np.full(len(data_holder.data), np.nan)
            dens[valid_rows.to_numpy()] = in_situ_density(
                psal, temp, depth, self.p_reference, lon, lat
            )

            data_holder.data = data_holder.data.with_columns(
                pl.when(valid_rows)
                .then(
                    pl.Series(dens).map_elements(
                        lambda x: None if np.isnan(x) else x, return_dtype=pl.Float64
                    )
                )
                .otherwise(None)
                .alias(self.col_to_set)
            )
        else:
            adm_logger.log_transformation(
                "No valid data rows for density calculation.", level=adm_logger.WARNING
            )


class PolarsAddDensity(PolarsTransformer):
    valid_data_structures = ("row",)
    practical_salinity = "Salinity CTD"
    temperature = "Temperature CTD"
    depth_col = "sample_depth_m"
    longitude_col = "sample_longitude_dd"
    latitude_col = "sample_latitude_dd"
    col_to_set = "in_situ_density"  # in kg/m3
    p_reference = 0  # reference pressure for pot. temperature set to 0 dbar.

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating density. Setting value to column {PolarsAddDensity.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.col_to_set in data_holder.data.columns:
            adm_logger.log_transformation(
                f"{self.col_to_set} already exists, values will be recalculated",
                level=adm_logger.INFO,
            )
        must_haves = [
            "visit_key",
            self.longitude_col,
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
        if (
            sum(
                [
                    data_holder.data.filter(
                        (pl.col("parameter") == self.practical_salinity)
                        & pl.col("value").is_not_null()
                    ).height
                    > 0,
                    data_holder.data.filter(
                        (pl.col("parameter") == self.temperature)
                        & pl.col("value").is_not_null()
                    ).height
                    > 0,
                ]
            )
            < 2
        ):
            adm_logger.log_transformation(
                f"Missing required {self.practical_salinity}, and/or {self.temperature}.",
                level=adm_logger.WARNING,
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
            adm_logger.log_transformation(
                f"Not enough {self.practical_salinity} and "
                f"{self.temperature} data to calculate from.",
                level=adm_logger.WARNING,
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


class PolarsAddOxygenSaturationWide(PolarsTransformer):
    valid_data_structures = ("column",)
    practical_salinity = "COPY_VARIABLE.Salinity CTD.o/oo psu"
    temperature = "COPY_VARIABLE.Temperature CTD.C"
    oxygen = "COPY_VARIABLE.Dissolved oxygen O2 CTD.ml/l"
    density_col = "COPY_VARIABLE.Derived in situ density.kg/m3"  # in kg/m3
    depth_col = "sample_depth_m"
    longitude_col = "sample_longitude_dd"
    latitude_col = "sample_latitude_dd"
    cols_to_set: ClassVar[list[str]] = [
        "COPY_VARIABLE.Derived oxygen at saturation.ml/l",
        "COPY_VARIABLE.Derived oxygen saturation.%",
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
            self.depth_col,
            self.longitude_col,
            self.latitude_col,
            self.practical_salinity,
            self.temperature,
            self.oxygen,
            self.density_col,
        ]
        if not all(col in data_holder.data.columns for col in must_haves):
            adm_logger.log_transformation(
                "Missing required columns: "
                f"{[col for col in must_haves if col not in data_holder.data.columns]}",
                level=adm_logger.WARNING,
            )
            return
        valid_rows = (
            data_holder.data[self.longitude_col]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.latitude_col]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.depth_col]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.practical_salinity]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.temperature]
            .cast(pl.Float64, strict=False)
            .is_not_null()
            & data_holder.data[self.oxygen].cast(pl.Float64, strict=False).is_not_null()
            & data_holder.data[self.density_col]
            .cast(pl.Float64, strict=False)
            .is_not_null()
        )

        if valid_rows.any():
            valid_data = data_holder.data.filter(valid_rows)
            practical_salinity = (
                valid_data[self.practical_salinity].cast(pl.Float64).to_numpy()
            )
            temperature = valid_data[self.temperature].cast(pl.Float64).to_numpy()
            oxygen = valid_data[self.oxygen].cast(pl.Float64).to_numpy()
            density = valid_data[self.density_col].cast(pl.Float64).to_numpy()
            depth = valid_data[self.depth_col].cast(pl.Float64).to_numpy()
            longitude = valid_data[self.longitude_col].cast(pl.Float64).to_numpy()
            latitude = valid_data[self.latitude_col].cast(pl.Float64).to_numpy()

            oxygen_values = np.full(len(data_holder.data), np.nan)
            oxygen_values[valid_rows.to_numpy()] = oxygen_from_umol_kg_2_ml_l(
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
            oxygen_values_percent = np.full(len(data_holder.data), np.nan)
            oxygen_values_percent[valid_rows.to_numpy()] = (
                oxygen / oxygen_values[valid_rows.to_numpy()] * 100
            )
            oxygen_values = np.where(np.isnan(oxygen_values), None, oxygen_values)
            oxygen_values_percent = np.where(
                np.isnan(oxygen_values_percent), None, oxygen_values_percent
            )

            data_holder.data = data_holder.data.with_columns(
                [
                    pl.when(valid_rows)
                    .then(
                        pl.Series(oxygen_values).map_elements(
                            lambda x: None if np.isnan(x) else x, return_dtype=pl.Float64
                        )
                    )
                    .otherwise(None)
                    .alias(self.cols_to_set[0]),
                    pl.when(valid_rows)
                    .then(
                        pl.Series(oxygen_values_percent).map_elements(
                            lambda x: None if np.isnan(x) else x, return_dtype=pl.Float64
                        )
                    )
                    .otherwise(None)
                    .alias(self.cols_to_set[1]),
                ]
            )
        else:
            adm_logger.log_transformation(
                "No valid data rows for oxygen calculations.", level=adm_logger.WARNING
            )


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
            sum(
                [
                    data_holder.data.filter(
                        (pl.col("parameter") == self.practical_salinity)
                        & pl.col("value").is_not_null()
                    ).height
                    > 0,
                    data_holder.data.filter(
                        (pl.col("parameter") == self.temperature)
                        & pl.col("value").is_not_null()
                    ).height
                    > 0,
                    data_holder.data.filter(
                        (pl.col("parameter") == self.oxygen)
                        & pl.col("value").is_not_null()
                    ).height
                    > 0,
                ]
            )
            < 3
        ):
            adm_logger.log_transformation(
                f"Missing required {self.practical_salinity}, "
                f"{self.temperature} and/or {self.oxygen}.",
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

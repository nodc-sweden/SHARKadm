import pathlib

import polars as pl

from sharkadm.config import get_import_matrix_mapper
from sharkadm.sharkadm_logger import adm_logger

from ..data.profile.base import PolarsProfileDataHolder
from .base import PolarsFileExporter


class ExportStandardFormat(PolarsFileExporter):
    valid_data_structures = ("profile",)

    initial_lines = (
        "//FORMAT=PROFILE",
        "//METADATA_DELIMITER=;",
        r"//DATA_DELIMITER=\t",
        "//ENCODING=cp1252",
    )

    metadata_order = (
        "visit_year",
        "sample_project_name",
        "sample_orderer_code",
        "sampling_laboratory_code",
        "analytical_laboratory_code",
        "visit_date",
        "sample_time",
        "sample_enddate",
        "sample_endtime",
        "platform_code",
        "expedition_id",
        "visit_id",
        "reported_station_name",
        "visit_reported_latitude",
        "visit_reported_longitude",
        "positioning_system_code",
        "water_depth_m",
        "visit_comment",
        "additional_sampling",
        "sampler_type_code",
        "instrument_id",
        "method_reference_code",
        "revision_date",
        "profile_file_name",
        "profile_file_name_db",
        "wind_direction_code",
        "wind_speed_ms",
        "air_temperature_degc",
        "air_pressure_hpa",
        "weather_observation_code",
        "cloud_observation_code",
        "wave_observation_code",
        "ice_observation_code",
        "INSTRUMENT_SERIE",
    )

    initial_data_columns = (
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "CRUISE",
        "STATION",
        "LATITUDE_DD",
        "LONGITUDE_DD",
    )

    FINAL_COLUMN_ORDER = (
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "CRUISE",
        "STATION",
        "LATITUDE_DD",
        "LONGITUDE_DD",
        "PRES_CTD [dbar]",
        "QV:SMHI:Q0_PRES_CTD",
        "QV:SMHI:PRES_CTD [dbar]",
        "DEPH [m]",
        "QV:SMHI:Q0_DEPH",
        "QV:SMHI:DEPH [m]",
        "TEMP_CTD [°C (ITS-90)]",
        "QV:SMHI:Q0_TEMP_CTD",
        "QV:SMHI:TEMP_CTD [°C (ITS-90)]",
        "TEMP2_CTD [°C (ITS-90)]",
        "QV:SMHI:Q0_TEMP2_CTD",
        "QV:SMHI:TEMP2_CTD [°C (ITS-90)]",
        "SALT_CTD [psu]",
        "QV:SMHI:Q0_SALT_CTD",
        "QV:SMHI:SALT_CTD [psu]",
        "SALT2_CTD [psu]",
        "QV:SMHI:Q0_SALT2_CTD",
        "QV:SMHI:SALT2_CTD [psu]",
        "CNDC_CTD [S/m]",
        "QV:SMHI:Q0_CNDC_CTD",
        "QV:SMHI:CNDC_CTD [S/m]",
        "CNDC2_CTD [S/m]",
        "QV:SMHI:Q0_CNDC2_CTD",
        "QV:SMHI:CNDC2_CTD [S/m]",
        "SIGMA_THETA_CTD [kg/m3]",
        "QV:SMHI:Q0_SIGMA_THETA_CTD",
        "QV:SMHI:SIGMA_THETA_CTD [kg/m3]",
        "SIGMA_THETA2_CTD [kg/m3]",
        "QV:SMHI:Q0_SIGMA_THETA2_CTD",
        "QV:SMHI:SIGMA_THETA2_CTD [kg/m3]",
        "DENS_CTD [kg/m3]",
        "QV:SMHI:Q0_DENS_CTD",
        "QV:SMHI:DENS_CTD [kg/m3]",
        "DENS2_CTD [kg/m3]",
        "QV:SMHI:Q0_DENS2_CTD",
        "QV:SMHI:DENS2_CTD [kg/m3]",
        "DOXY_CTD [ml/l]",
        "QV:SMHI:Q0_DOXY_CTD",
        "QV:SMHI:DOXY_CTD [ml/l]",
        "DOXY2_CTD [ml/l]",
        "QV:SMHI:Q0_DOXY2_CTD",
        "QV:SMHI:DOXY2_CTD [ml/l]",
        "DOXY_SAT_CTD [%]",
        "QV:SMHI:Q0_DOXY_SAT_CTD",
        "QV:SMHI:DOXY_SAT_CTD [%]",
        "DOXY_SAT2_CTD [%]",
        "QV:SMHI:Q0_DOXY_SAT2_CTD",
        "QV:SMHI:DOXY_SAT2_CTD [%]",
        "SVEL_CTD [m/s]",
        "QV:SMHI:Q0_SVEL_CTD",
        "QV:SMHI:SVEL_CTD [m/s]",
        "SVEL2_CTD [m/s]",
        "QV:SMHI:Q0_SVEL2_CTD",
        "QV:SMHI:SVEL2_CTD [m/s]",
        "CHLFLUO_CTD [mg/m3]",
        "QV:SMHI:Q0_CHLFLUO_CTD",
        "QV:SMHI:CHLFLUO_CTD [mg/m3]",
        "PHYC_CTD [ppb]",
        "QV:SMHI:Q0_PHYC_CTD",
        "QV:SMHI:PHYC_CTD [ppb]",
        "TURB_CTD [NTU]",
        "QV:SMHI:Q0_TURB_CTD",
        "QV:SMHI:TURB_CTD [NTU]",
        "PAR_CTD [µE/(cm2 sec)]",
        "QV:SMHI:Q0_PAR_CTD",
        "QV:SMHI:PAR_CTD [µE/(cm2 sec)]",
        "ALTM_CTD [m]",
        "QV:SMHI:Q0_ALTM_CTD",
        "QV:SMHI:ALTM_CTD [m]",
        "DESC_RATE_CTD [m/s]",
        "QV:SMHI:Q0_DESC_RATE_CTD",
        "QV:SMHI:DESC_RATE_CTD [m/s]",
        "PUMP_CTD []",
        "QV:SMHI:Q0_PUMP_CTD",
        "QV:SMHI:PUMP_CTD []",
        "SALT_DIFF_CTD [psu]",
        "QV:SMHI:Q0_SALT_DIFF_CTD",
        "QV:SMHI:SALT_DIFF_CTD [psu]",
        "TEMP_DIFF_CTD [°C (ITS-90)]",
        "QV:SMHI:Q0_TEMP_DIFF_CTD",
        "QV:SMHI:TEMP_DIFF_CTD [°C (ITS-90)]",
        "SCAN_BIN_CTD []",
        "QV:SMHI:Q0_SCAN_BIN_CTD",
        "QV:SMHI:SCAN_BIN_CTD []",
        "SCAN_CTD []",
        "QV:SMHI:Q0_SCAN_CTD",
        "QV:SMHI:SCAN_CTD []",
        "COMNT_SAMP:TEXT",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mapper = get_import_matrix_mapper("profile", "profile")

    @staticmethod
    def get_exporter_description() -> str:
        return "Writes profile data to standard format"

    def _export(self, data_holder: PolarsProfileDataHolder) -> None:
        if not data_holder.sensor_info:
            adm_logger.log_export(
                "So sensor_info loaded. Will create without this information"
            )
        for (source,), df in data_holder.data.group_by("source"):
            all_lines = []
            meta_lines = self._get_metadata_lines(
                data_holder=data_holder, source=str(source)
            )
            sensor_info_lines = self._get_sensor_info_lines(
                data_holder=data_holder, source=source
            )
            instrument_info_lines = self._get_instrument_info_lines(
                data_holder=data_holder, source=source
            )
            data_lines = self._get_data_lines(data_holder=data_holder, df=df)

            all_lines.extend(self.initial_lines)
            all_lines.extend(meta_lines)
            all_lines.extend(sensor_info_lines)
            all_lines.append("//INFORMATION;")
            all_lines.extend(instrument_info_lines)
            all_lines.extend(data_lines)

            export_path = (
                self.export_directory / pathlib.Path(source).with_suffix(".txt").name
            )
            print(f"{export_path=}")
            with open(export_path, "w", encoding="cp1252") as fid:
                fid.write("\n".join(all_lines))

    def _get_metadata_columns_in_order(
        self, data_holder: PolarsProfileDataHolder, key: str
    ) -> list[str]:
        meta_columns = [
            col for col in self.metadata_order if col in data_holder.metadata_by_key[key]
        ]
        return meta_columns

    def _get_metadata_lines(
        self, data_holder: PolarsProfileDataHolder, source: str
    ) -> list[str]:
        lines = []
        key = pathlib.Path(source).stem
        columns_in_order = self._get_metadata_columns_in_order(data_holder, key)
        for meta_par in columns_in_order:
            trans_meta_par = self._mapper.get_external_name(meta_par)
            value = data_holder.metadata_by_key[key][meta_par]
            lines.append(f"//METADATA;{trans_meta_par};{value}")
            if (
                trans_meta_par == "FILE_NAME"
                and "FILE_NAME_DATABASE" not in columns_in_order
            ):
                val = pathlib.Path(value).with_suffix(".txt").name
                lines.append(f"//METADATA;{'FILE_NAME_DATABASE'};{val}")
        return lines

    def _get_sensor_info_lines(
        self, data_holder: PolarsProfileDataHolder, source: str
    ) -> list[str]:
        if not data_holder.sensor_info:
            return []
        lines = []
        key = pathlib.Path(source).stem
        for info in data_holder.sensor_info[key].data.to_dicts():
            if not lines:
                line = ["//SENSORINFO", *list(info.keys())]
                lines.append(";".join(line))
            line = ["//SENSORINFO", *list(info.values())]
            lines.append(";".join(line))
        return lines

    def _get_instrument_info_lines(
        self, data_holder: PolarsProfileDataHolder, source: str
    ) -> list[str]:
        lines = []
        key = pathlib.Path(source).stem
        for item in data_holder.data_sources[key].metadata_lines:
            if "END" in item:
                continue
            lines.append(f"//INSTRUMENT_METADATA;{item.strip()}")
        return lines

    def _get_data_lines(
        self, data_holder: PolarsProfileDataHolder, df: pl.DataFrame
    ) -> list[str]:
        key = pathlib.Path(df["source"][0]).stem
        updated_df = self._add_initial_data_columns(df)
        data_cols = [
            col
            for col in data_holder.data.columns
            if col not in data_holder.metadata_by_key[key]
        ]
        data_cols = [
            data_holder.header_mapper.get_external_name(col) for col in data_cols
        ]
        data_cols = self.initial_data_columns + data_cols
        remove_cols = ["row_number"]
        data_cols = [col for col in data_cols if col not in remove_cols]

        unit_mapper = data_holder.get_unit_mapper()
        unit_mapper["PRES_CTD"] = "[dbar]"

        cols_to_add = []
        new_cols = []
        for col in data_cols:
            unit = unit_mapper.get(col, "")
            col_with_unit = f"{col} {unit}".strip()
            new_cols.append(col_with_unit)
            if col in self.initial_data_columns:
                continue
            # col_no_unit = col.split("[")[0].strip()
            q0_col = f"QV:SMHI:Q0_{col}"
            q_col = f"QV:SMHI:{col} {unit}".strip()
            cols_to_add.append(pl.col(col).alias(col_with_unit))
            cols_to_add.append(pl.lit("").alias(q0_col))
            cols_to_add.append(pl.lit("").alias(q_col))
            new_cols.append(q0_col)
            new_cols.append(q_col)
        print(f"1:         {new_cols=}")
        final_df = updated_df.with_columns(cols_to_add)[new_cols]
        print(f"1: {final_df.columns=}")

        final_df = final_df.with_columns(pl.col("MONTH").str.zfill(2).alias("MONTH"))
        col_order = self._get_final_column_order(final_df)
        print(f"2:        {col_order=}")
        final_df = final_df[col_order]
        print(f"2: {final_df.columns=}")

        lines = list()
        lines.append("\t".join(final_df.columns))
        for line in final_df.iter_rows():
            lines.append("\t".join(line))
        return lines

    def _get_final_column_order(self, df: pl.DataFrame) -> list:
        columns = [col for col in self.FINAL_COLUMN_ORDER if col in df.columns]
        add_columns = [col for col in df.columns if col not in columns]
        columns.extend(add_columns)
        return columns

    def _add_initial_data_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        new_df = df.with_columns(
            [
                pl.col("datetime").dt.year().cast(str).alias("YEAR"),
                pl.col("datetime").dt.month().cast(str).alias("MONTH"),
                pl.col("datetime").dt.day().cast(str).alias("DAY"),
                pl.col("datetime").dt.hour().cast(str).alias("HOUR"),
                pl.col("datetime").dt.minute().cast(str).alias("MINUTE"),
                pl.col("datetime").dt.second().cast(str).alias("SECOND"),
                # pl.col("expedition_id").alias("CRUISE"),
                pl.concat_str(
                    [
                        pl.col("datetime").dt.year().cast(str),
                        pl.col("platform_code"),
                        pl.col("expedition_id"),
                    ],
                    separator="_",
                ).alias("CRUISE"),
                pl.col("reported_station_name").alias("STATION"),
                pl.col("sample_latitude_dd").alias("LATITUDE_DD"),
                pl.col("sample_longitude_dd").alias("LONGITUDE_DD"),
            ]
        )
        return new_df
        # "YEAR",
        # "MONTH",
        # "DAY",
        # "HOUR",
        # "MINUTE",
        # "SECOND",
        # "CRUISE",
        # "STATION",
        # "LATITUDE_DD",
        # "LONGITUDE_DD",

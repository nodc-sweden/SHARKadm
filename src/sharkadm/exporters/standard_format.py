import pathlib
import re

import polars as pl

from sharkadm import utils

from .base import DataHolderProtocol, PolarsExporter, PolarsFileExporter
from ..data import PolarsDataHolder
from ..data.profile.base import PolarsProfileDataHolder
from ..utils.paths import get_next_incremented_file_path


class PolarsStandardFormat(PolarsFileExporter):
    valid_data_structures = ("profile",)

    def __init__(
        self,
        export_directory: str | pathlib.Path | None = None,
        export_file_name: str | pathlib.Path | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not export_directory:
            export_directory = utils.get_export_directory("standard_format")
        self._export_directory = pathlib.Path(export_directory)
        self._export_file_name = export_file_name
        self._encoding = kwargs.get("encoding", "cp1252")

        self._initial_rows = [
            r"// FORMAT=PROFILE",
            r"// METADATA_DELIMITER=;",
            r"// DATA_DELIMITER =\t",
            r"// ENCODING=cp1252",
        ]

    @staticmethod
    def get_exporter_description() -> str:
        return ""

    def _export(self, data_holder: PolarsProfileDataHolder) -> None:
        metadata = data_holder.metadata_original_columns
        for (date, time), data in data_holder.data.group_by("visit_date",
                                                            "sample_time"):
            stem = pathlib.Path(data[0, "source"]).stem
            meta = metadata[(date, time)]
            data_cols = [col for col in data_holder.data.columns if not meta.get(col)]
            meta_rows = self._get_metadata_rows(meta)

            data = self._add_columns(data)
            data = self._remove_columns(data)
            data = self._add_qf_columns(data, data_cols)
            data = self._reorder_data(data, data_cols)

            # d_data = self._get_data(data=data, data_cols=data_cols, data_holder=data_holder)
            # d_data = self._get_data(data=data, data_holder=data_holder)
            # d_data = self._get_data(data=data, data_cols=ordered_cols)

            path = self._export_directory / f"{stem}.txt"
            if path.exists():
                path = get_next_incremented_file_path(path)
            header_lines = self._initial_rows + meta_rows
            with open(path, "w", encoding="cp1252") as fid:
                fid.write("\n".join(header_lines))
                fid.write("\n")
            data.to_pandas().to_csv(path, mode="a", sep="\t", index=False, encoding="cp1252")

    def _add_columns(self, data: pl.DataFrame) -> pl.DataFrame:
        data = data.with_columns(
            YEAR=pl.col("visit_date").str.slice(0, length=4),
            MONTH=pl.col("visit_date").str.slice(5, length=2),
            DAY=pl.col("visit_date").str.slice(8, length=2),
            HOUR=pl.col("sample_time").str.slice(0, length=2),
            MINUTE=pl.col("sample_time").str.slice(3, length=2),
            SECOND=pl.col("sample_time").str.slice(6, length=2),
            STATION=pl.col("reported_station_name"),
            LATITUDE_DD=pl.col("visit_reported_latitude"),
            LONGITUDE_DD=pl.col("visit_reported_longitude"),
            CRUISE=pl.lit(""),
            FILE_NAME=pl.col("source").str.split('\\').list.last()
        )
        if "cruise_id" in data.columns:
            data = data.with_columns(
                CRUISE=pl.col("cruise_id"),
            )
        return data

    def _remove_columns(self, data: pl.DataFrame) -> pl.DataFrame:
        drop_columns = [
            "source",
            "reported_station_name",
            "visit_reported_latitude",
            "visit_reported_longitude",
        ]
        data = data.drop(*drop_columns)
        return data

    def _add_qf_columns(self, data: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
        args = []
        for col in columns:
            args.extend([
                pl.lit("").alias(self._get_qcol_name(col)),
                pl.lit("").alias(self._get_q0col_name(col)),
            ])
        data = data.with_columns(args)
        return data

    def _reorder_data(self, data: pl.DataFrame, data_columns: list[str]) -> pl.DataFrame:
        initial_columns = [
            "YEAR",
            "MONTH",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            "CRUISE",
            "STATION",
            "LATITUDE_DD",
            "LONGITUDE_DD"
            ]
        pres_cols = []
        depth_cols = []
        exclude_cols = []
        for col in data.columns:
            if "QV:" in col:
                exclude_cols.append(col)
            if "CTD" in col:
                continue
            if "PRES" in col:
                pres_cols.append(col)
                pres_cols.append(self._get_qcol_name(col))
                pres_cols.append(self._get_q0col_name(col))
            elif "DEPH" in col:
                depth_cols.append(col)
                depth_cols.append(self._get_qcol_name(col))
                depth_cols.append(self._get_q0col_name(col))
        exclude_cols = initial_columns + pres_cols + depth_cols
        new_order = exclude_cols
        for col in [c for c in data_columns if c in data.columns]:
            if col in exclude_cols:
                continue
            new_order.append(col)
            new_order.append(self._get_qcol_name(col))
            new_order.append(self._get_q0col_name(col))
        return data[new_order]

    def _get_qcol_name(self, col: str) -> str:
        return f"QV:SMHI:{col}"

    def _get_q0col_name(self, col: str) -> str:
        return f"QV:SMHI:Q0_{col}"

    def _get_data(self, data: pl.DataFrame, data_cols: list[str], data_holder: PolarsDataHolder) -> pl.DataFrame:

        ordered_cols = self._get_reordered_columns(data_cols)
        d_data = data[ordered_cols]
        d_data.columns = self._get_translated_columns(ordered_cols)
        new_data = dict(
            YEAR=data["datetime"].apply(lambda x: str(x.year)),
            MONTH=data["datetime"].apply(lambda x: str(x.month).zfill(2)),
            DAY=data["datetime"].apply(lambda x: str(x.day).zfill(2)),
            HOUR=data["datetime"].apply(lambda x: str(x.hour).zfill(2)),
            MINUTE=data["datetime"].apply(lambda x: str(x.minute).zfill(2)),
            SECOND=data["datetime"].apply(lambda x: str(x.second).zfill(2)),
            CRUISE=data["cruise_id"],
            STATION=data["reported_station_name"],
            LATITUDE_DD=data["sample_latitude_dd"],
            LONGITUDE_DD=data["sample_longitude_dd"],
        )
        df = d_data.copy()
        df.reset_index(drop=True, inplace=True)
        add_df = pd.DataFrame.from_dict(new_data)
        add_df.reset_index(drop=True, inplace=True)
        d_data = pd.concat([add_df, df], axis=1)
        # d_data.insert(0, column='LONGITUDE_DD', value=data['sample_longitude_dd'])
        # d_data.insert(0, column='LATITUDE_DD', value=data['sample_latitude_dd'])
        # d_data.insert(0, column='STATION', value=data['reported_station_name'])
        # d_data.insert(0, column='CRUISE', value=data['cruise_id'])
        # d_data.insert(
        #     0, column='SECOND',
        #     value=data['datetime'].apply(lambda x: str(x.second).zfill(2))
        # )
        # d_data.insert(
        #     0, column='MINUTE',
        #     value=data['datetime'].apply(lambda x: str(x.minute).zfill(2))
        # )
        # d_data.insert(
        #     0, column='HOUR',
        #     value=data['datetime'].apply(lambda x: str(x.hour).zfill(2))
        # )
        # d_data.insert(
        #     0, column='DAY', value=data['datetime'].apply(lambda x: str(x.day).zfill(2))
        # )
        # d_data.insert(
        #     0, column='MONTH',
        #     value=data['datetime'].apply(lambda x: str(x.month).zfill(2))
        # )
        # d_data.insert(
        #     0, column='YEAR', value=data['datetime'].apply(lambda x: str(x.year))
        # )
        # print(f'3::: {d_data.columns[:15]=}')
        return d_data

    # @staticmethod
    # def _get_reordered_columns(columns: list[str]) -> list[str]:
    #     pres_cols = []
    #     depth_cols = []
    #     exclude_cols = []
    #     for col in columns:
    #         if "QV:" in col:
    #             exclude_cols.append(col)
    #         if "CTD" in col:
    #             continue
    #         if "PRES" in col:
    #             pres_cols.append(col)
    #         elif "DEPH" in col:
    #             depth_cols.append(col)
    #     exclude_cols = pres_cols + depth_cols
    #     new_order = []
    #     for col in columns:
    #         if col in exclude_cols:
    #             return
    #     new_order = (
    #         pres_cols + depth_cols + [col for col in columns if col not in exclude_cols]
    #     )
    #     return new_order

    @staticmethod
    def _get_metadata_rows(metadata: dict[str, str]) -> list[str]:
        meta_rows = []
        for col, value in metadata.items():
            meta = f"//METADATA;{col};{value}"
            meta_rows.append(meta)
        return meta_rows

    # @staticmethod
    # def _get_data_columns(columns: list[str]) -> list[str]:
    #     q_columns = []
    #     for col in columns[:]:
    #         if col.startswith("Q_"):
    #             q_columns.append(col)
    #         elif col.startswith("Q0_"):
    #             q_columns.append(col)
    #         elif f"Q_{col}" in columns:
    #             q_columns.append(col)
    #     return q_columns

    # @staticmethod
    # def _get_translated_columns(columns: list[str]) -> list[str]:
    #     new_columns = []
    #     for col in columns:
    #         if col.startswith("Q_"):
    #             new_columns.append(f"QV:SMHI:{col[2:]}")
    #         elif col.startswith("Q0_"):
    #             new_columns.append(f"QV:SMHI:{col}")
    #         else:
    #             new_columns.append(col)
    #     return new_columns

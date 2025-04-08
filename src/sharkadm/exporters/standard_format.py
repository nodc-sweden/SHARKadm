import pathlib

import pandas as pd

from sharkadm import utils

from .base import DataHolderProtocol, Exporter


class StandardFormat(Exporter):
    valid_data_types = ("physicalchemical",)

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

    def _export(self, data_holder: DataHolderProtocol) -> None:
        groups = data_holder.data.groupby("visit_key")
        data_cols = self._get_data_columns(data_holder.data.columns)
        metadata_cols = [col for col in data_holder.data.columns if col not in data_cols]
        ordered_cols = self._get_reordered_columns(data_cols)
        for _id, data in groups:
            meta_rows = self._get_metadata_rows(data, metadata_cols)
            d_data = self._get_data(data=data, data_cols=ordered_cols)
            path = self._export_directory / f"{_id}.txt"
            header_lines = self._initial_rows + meta_rows
            with open(path, "w") as fid:
                fid.write("\n".join(header_lines))
                fid.write("\n")
            d_data.to_csv(path, mode="a", sep="\t", index=False)

    def _get_data(self, data: pd.DataFrame, data_cols: list[str]) -> pd.DataFrame:
        d_data = data[data_cols]
        d_data.columns = self._get_translated_columns(data_cols)
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

    @staticmethod
    def _get_reordered_columns(columns: list[str]) -> list[str]:
        pres_cols = []
        depth_cols = []
        for col in columns:
            if "CTD" in col:
                continue
            if "PRES" in col:
                pres_cols.append(col)
            elif "DEPH" in col:
                depth_cols.append(col)
        exclude_cols = pres_cols + depth_cols
        new_order = (
            pres_cols + depth_cols + [col for col in columns if col not in exclude_cols]
        )
        return new_order

    @staticmethod
    def _get_metadata_rows(data: pd.DataFrame, columns: list[str]) -> list[str]:
        meta_rows = []
        for col in columns:
            meta_value = ", ".join(set(data[col].astype(str)))
            meta = f"//METADATA;{col};{meta_value}"
            meta_rows.append(meta)
        return meta_rows

    @staticmethod
    def _get_data_columns(columns: list[str]) -> list[str]:
        q_columns = []
        for col in columns[:]:
            if col.startswith("Q_"):
                q_columns.append(col)
            elif col.startswith("Q0_"):
                q_columns.append(col)
            elif f"Q_{col}" in columns:
                q_columns.append(col)
        return q_columns

    @staticmethod
    def _get_translated_columns(columns: list[str]) -> list[str]:
        new_columns = []
        for col in columns:
            if col.startswith("Q_"):
                new_columns.append(f"QV:SMHI:{col[2:]}")
            elif col.startswith("Q0_"):
                new_columns.append(f"QV:SMHI:{col}")
            else:
                new_columns.append(col)
        return new_columns

import pathlib

import polars as pl

from sharkadm.data.archive import metadata
from sharkadm.data.profile import PolarsCnvDataHolder, sensorinfo
from sharkadm.transformers.base import PolarsTransformer


class PolarsAddMetadataToProfileData(PolarsTransformer):
    valid_data_structures = ("profile",)

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: PolarsCnvDataHolder) -> None:
        meta = self.get_metadata_from_single_txt_file(data_holder)
        if not meta:
            meta = self.get_metadata_from_multiple_txt_file(data_holder)
        data_holder.add_metadata(meta)

    def get_metadata_from_single_txt_file(
        self, data_holder: PolarsCnvDataHolder
    ) -> metadata.Metadata | None:
        meta_path = self._get_metadata_txt_path(data_holder.parent_directory)
        if not meta_path:
            return
        meta = metadata.Metadata.from_txt_file(
            meta_path, mapper=data_holder.header_mapper
        )
        return meta

    def get_metadata_from_multiple_txt_file(
        self, data_holder: PolarsCnvDataHolder
    ) -> metadata.Metadata | None:
        meta_paths = self._get_metadata_txt_paths(data_holder.parent_directory)
        if not meta_paths:
            return
        meta_dfs = []
        for meta_path in meta_paths:
            meta = metadata.Metadata.from_txt_file(
                meta_path, mapper=data_holder.header_mapper
            )
            meta_dfs.append(meta.data)
        meta_df = pl.concat(meta_dfs)
        return metadata.Metadata(meta_df)

    def _get_metadata_txt_path(self, directory: pathlib.Path) -> pathlib.Path | None:
        path = directory / "metadata.txt"
        if path.exists():
            return path

    def _get_metadata_txt_paths(self, directory: pathlib.Path) -> list[pathlib.Path]:
        paths = []
        for path in directory.iterdir():
            if path.suffix == ".metadata":
                paths.append(path)
        return paths


class PolarsLoadSensorInfoToProfileData(PolarsTransformer):
    valid_data_structures = ("profile",)

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: PolarsCnvDataHolder) -> None:
        info = self.get_sensor_info_from_parent_directory(data_holder)
        data_holder.set_sensor_info(info)

    def get_sensor_info_from_parent_directory(
        self, data_holder: PolarsCnvDataHolder
    ) -> dict[str, sensorinfo.Sensorinfo]:
        info = dict()
        general_path = self._get_general_sensor_info(data_holder.parent_directory)
        if general_path:
            info["general"] = sensorinfo.Sensorinfo.from_sensorinfo_file(general_path)
        paths = self._get_sensorinfo_paths(data_holder.parent_directory)
        for path in paths:
            print(f"{path=}")
            info[path.stem] = sensorinfo.Sensorinfo.from_sensorinfo_file(path)
        return info

    def _get_general_sensor_info(self, directory: pathlib.Path) -> pathlib.Path | None:
        path = directory / "sensorinfo.txt"
        if path.exists():
            return path

    def _get_sensorinfo_paths(self, directory: pathlib.Path) -> list[pathlib.Path]:
        paths = []
        for path in directory.iterdir():
            if path.suffix == ".sensorinfo":
                paths.append(path)
        return paths

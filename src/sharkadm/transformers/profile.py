import pathlib

from sharkadm.data import PolarsDataHolder
from sharkadm.data.profile import PolarsCnvDataHolder
from sharkadm.sharkadm_logger import adm_logger

from sharkadm.data.archive import metadata
from sharkadm.transformers.base import PolarsTransformer


class PolarsAddMetadataToProfileData(PolarsTransformer):
    valid_data_structures = ("profile", )

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds info if red listed. Red listed species are marked with Y"

    def _transform(self, data_holder: PolarsCnvDataHolder) -> None:
        meta_path = self._get_metadata_path(data_holder.parent_directory)
        if not meta_path:
            print("No file found")
            return
        print(f"{data_holder.header_mapper=}")
        meta = metadata.Metadata.from_txt_file(meta_path, mapper=data_holder.header_mapper)
        data_holder.add_metadata(meta)

    def _get_metadata_path(self, directory: pathlib.Path) -> pathlib.Path | None:
        for path in directory.iterdir():
            if path.name == "metadata.txt":
                return path

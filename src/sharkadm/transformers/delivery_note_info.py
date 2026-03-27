import polars as pl

from sharkadm.config import adm_config_paths
from sharkadm.data.archive import PolarsArchiveDataHolder
from sharkadm.data.data_holder import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer
from sharkadm.utils import yaml_data


class PolarsAddDeliveryNoteInfo(PolarsTransformer):
    physical_chemical_keys = (
        "PhysicalChemical".lower(),
        "Physical and Chemical".lower(),
    )

    def __init__(self, *columns: str, overwrite: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._columns = columns
        self._overwrite = overwrite
        self._status_config = yaml_data.load_yaml(
            adm_config_paths("delivery_note_status"), encoding="utf8"
        )

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds info from delivery_note"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not hasattr(data_holder, "delivery_note"):
            self._log(
                f"No delivery note found for data holder {data_holder}",
                level=adm_logger.WARNING,
            )
            return
        self._add_delivery_note_info(data_holder)
        self._add_status(data_holder)

    def _add_delivery_note_info(
        self, data_holder: PolarsDataHolder | PolarsArchiveDataHolder
    ):
        ops_to_run = []
        for key in data_holder.delivery_note.fields:
            if key in data_holder.data.columns and not self._overwrite:
                continue
            ops_to_run.append(pl.lit(str(data_holder.delivery_note[key])).alias(key))
        data_holder.data = data_holder.data.with_columns(ops_to_run)

    def _add_status(self, data_holder: PolarsDataHolder | PolarsArchiveDataHolder):
        if not hasattr(data_holder, "delivery_note"):
            adm_logger.log_workflow(
                "Could not add status. No delivery note found!",
                level=adm_logger.WARNING,
                item=data_holder.dataset_name,
            )
            return
        checked_by = data_holder.delivery_note["DATA KONTROLLERAD AV"]
        if not checked_by:
            self._log(
                'Could not set "status" and "checked". Missing information in '
                "delivery_note: data kontrollerad av",
                level=adm_logger.WARNING,
            )
            return
        data = dict()
        if data_holder.data_type.lower() in self.physical_chemical_keys:
            data = self._status_config["default_physical_chemical"]
        else:
            if checked_by == r"Leverantör":
                data = self._status_config["deliverer"]
            elif checked_by == r"Leverantör och Datavärd":
                data = self._status_config["deliverer_and_datahost"]

        data_holder.data = data_holder.data.with_columns(
            [
                pl.lit(data["check_status_sv"]).alias("check_status_sv"),
                pl.lit(data["check_status_en"]).alias("check_status_en"),
                pl.lit(data["data_checked_by_sv"]).alias("data_checked_by_sv"),
                pl.lit(data["data_checked_by_en"]).alias("data_checked_by_en"),
            ]
        )

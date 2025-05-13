import polars as pl

from sharkadm.config import adm_config_paths
from sharkadm.data.archive import ArchiveDataHolder
from sharkadm.data.archive.archive_data_holder import PolarsArchiveDataHolder
from sharkadm.data.data_holder import PandasDataHolder, PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import Transformer
from sharkadm.utils import yaml_data


class AddDeliveryNoteInfo(Transformer):
    physical_chemical_keys = (
        "PhysicalChemical".lower(),
        "Physical and Chemical".lower(),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._status_config = yaml_data.load_yaml(
            adm_config_paths("delivery_note_status"), encoding="utf8"
        )

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds info from delivery_note"

    def _transform(self, data_holder: PandasDataHolder | ArchiveDataHolder) -> None:
        if not hasattr(data_holder, "delivery_note"):
            self._log(
                f"No delivery note found for data holder {data_holder}",
                level=adm_logger.WARNING,
            )
            return
        self._add_delivery_note_info(data_holder)
        self._add_status(data_holder)

    def _add_delivery_note_info(self, data_holder: PandasDataHolder | ArchiveDataHolder):
        for key in data_holder.delivery_note.fields:
            if key in data_holder.data and any(data_holder.data[key]):
                self._log(
                    f"Not setting info from delivery_note. "
                    f"{key} already a column with data.",
                    level=adm_logger.DEBUG,
                )
                continue
            self._log(
                f"Adding {key} info from delivery_note",
                item=data_holder.delivery_note[key],
            )
            data_holder.data[key] = str(data_holder.delivery_note[key])
            # data_holder.data.loc[:, key] = data_holder.delivery_note[key]

    def _add_status(self, data_holder: PandasDataHolder | ArchiveDataHolder):
        if not hasattr(data_holder, "delivery_note"):
            adm_logger.log_workflow(
                "Could not add status. No delivery note found!",
                level=adm_logger.WARNING,
                item=data_holder.dataset_name,
            )
            return
        checked_by = data_holder.delivery_note["data kontrollerad av"]
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
                raise
            elif checked_by == r"Leverantör och Datavärd":
                data = self._status_config["deliverer_and_datahost"]
        data_holder.data["check_status_sv"] = data["check_status_sv"]
        data_holder.data["check_status_en"] = data["check_status_en"]
        data_holder.data["data_checked_by_sv"] = data["data_checked_by_sv"]
        data_holder.data["data_checked_by_en"] = data["data_checked_by_en"]


class AddStatus(Transformer):
    physical_chemical_keys = (
        "PhysicalChemical".lower(),
        "Physical and Chemical".lower(),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._config = yaml_data.load_yaml(adm_config_paths("delivery_note_status"))

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds status columns"

    def _transform(self, data_holder: ArchiveDataHolder) -> None:
        if not hasattr(data_holder, "delivery_note"):
            adm_logger.log_workflow(
                "Could not add status. No delivery note found!",
                level=adm_logger.WARNING,
                item=data_holder.dataset_name,
            )
            return
        checked_by = data_holder.delivery_note["data kontrollerad av"]
        if not checked_by:
            self._log(
                'Could not set "status" and "checked". '
                "Missing information in delivery_note: data kontrollerad av",
                level=adm_logger.WARNING,
            )
            return
        data = dict()
        if data_holder.data_type.lower() in self.physical_chemical_keys:
            data = self._config["default_physical_chemical"]
        else:
            if checked_by == r"Leverantör":
                data = self._config["deliverer"]
            elif checked_by == r"Leverantör och Datavärd":
                data = self._config["deliverer_and_datahost"]
        data_holder.data["check_status_sv"] = data["check_status_sv"]
        data_holder.data["check_status_en"] = data["check_status_en"]
        data_holder.data["data_checked_by_sv"] = data["data_checked_by_sv"]
        data_holder.data["data_checked_by_en"] = data["data_checked_by_en"]


class PolarsAddDeliveryNoteInfo(Transformer):
    physical_chemical_keys = (
        "PhysicalChemical".lower(),
        "Physical and Chemical".lower(),
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._status_config = yaml_data.load_yaml(
            adm_config_paths("delivery_note_status"), encoding="utf8"
        )

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds info from delivery_note"

    def _transform(self, data_holder: PolarsDataHolder | PolarsArchiveDataHolder) -> None:
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
            # if key in data_holder.data and any(data_holder.data[key]):
            #     self._log(
            #         f"Not setting info from delivery_note. "
            #         f"{key} already a column with data.",
            #         level=adm_logger.DEBUG,
            #     )
            #     continue
            # self._log(
            #     f"Adding {key} info from delivery_note",
            #     item=data_holder.delivery_note[key],
            # )
            ops_to_run.append(pl.lit(str(data_holder.delivery_note[key])).alias(key))
            # data_holder.data = data_holder.data.with_columns(
            #     pl.lit(str(data_holder.delivery_note[key])).alias(key)
            # )
        data_holder.data = data_holder.data.with_columns(ops_to_run)

    def _add_status(self, data_holder: PolarsDataHolder | PolarsArchiveDataHolder):
        if not hasattr(data_holder, "delivery_note"):
            adm_logger.log_workflow(
                "Could not add status. No delivery note found!",
                level=adm_logger.WARNING,
                item=data_holder.dataset_name,
            )
            return
        checked_by = data_holder.delivery_note["data kontrollerad av"]
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
                pl.lit(data["check_status_sv"]).alias("check_status_sv"),
                pl.lit(data["check_status_sv"]).alias("check_status_sv"),
            ]
        )

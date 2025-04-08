import pandas as pd

from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Transformer

try:
    import nodc_dyntaxa
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class AddRedList(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")

    col_to_set = "red_listed"
    source_cols = ("dyntaxa_id", "scientific_name", "reported_scientific_name")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mapped = {}
        self.red_list_obj = nodc_dyntaxa.get_red_list_object()

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds info if red listed. Red listed species are marked with Y"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(
                f"Adding column {self.col_to_set}", level=adm_logger.DEBUG
            )
            data_holder.data[self.col_to_set] = ""
        data_holder.data[self.col_to_set] = data_holder.data.apply(
            lambda row: self._add(row), axis=1
        )

    def _add(self, row: pd.Series) -> str:
        for col in self.source_cols:
            value = row[col]
            if not value.strip():
                continue
            info = self.mapped.get(value)
            if info:
                return "Y"
            info = self.red_list_obj.get_info(value)
            if info:
                self.mapped[value] = True
                adm_logger.log_transformation(
                    f"{value} is marked as red listed", level=adm_logger.INFO
                )
                return "Y"
        return ""

import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import PolarsTransformer

try:
    import nodc_dyntaxa
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class AddRedList(PolarsTransformer):
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

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_empty_col_to_set(data_holder)
        for col in self.source_cols:
            for (name,), df in data_holder.data.group_by(col):
                if not self.red_list_obj.get_info(name):
                    continue
                boolean = pl.col(col) == name
                data_holder.data = data_holder.data.with_columns(
                    pl.when(boolean)
                    .then(pl.lit("Y"))
                    .otherwise(pl.col(self.col_to_set))
                    .alias(self.col_to_set)
                )
                self._log(
                    f"{name} in column {col} is set as red listed ({len(df)} places)"
                )

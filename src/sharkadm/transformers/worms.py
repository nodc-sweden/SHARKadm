from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import PolarsTransformer

nodc_worms = None
try:
    import nodc_worms

    taxa_worms = nodc_worms.get_taxa_worms_object()
    translate_worms = nodc_worms.get_translate_worms_object()
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class PolarsAddWormsScientificName(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "reported_scientific_name"
    col_to_set = "worms_scientific_name"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddWormsScientificName.col_to_set} translated from nodc_worms. "
            f"Source column is {PolarsAddWormsScientificName.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_worms:
            self._log(
                "Could not add worms scientific name. "
                "Package nodc-worms not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        self._add_empty_col_to_set(data_holder)
        for (name,), df in data_holder.data.group_by(self.source_col):
            new_name = translate_worms.get(str(name))
            if new_name:
                self._log(
                    f"Translated using {translate_worms.source}. "
                    f"Reported name via dyntaxa: {name} "
                    f"Translated to: {new_name} ({len(df)} rows)"
                )
                # self._log(f"Translate worms: {name} -> {new_name} ({len(df)} places)")
            else:
                new_name = name
            self._add_to_col_to_set(data_holder, name, new_name)


class PolarsAddWormsAphiaId(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "worms_scientific_name"
    col_to_set = "worms_aphia_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddWormsAphiaId.col_to_set} "
            f"from {PolarsAddWormsAphiaId.source_col}"
        )

    @adm_logger.log_time
    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_worms:
            self._log(
                "Could not add aphia id. Package nodc-worms not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        if self.col_to_set not in data_holder.data.columns:
            self._log(f"Adding column {self.col_to_set}", level=adm_logger.DEBUG)
            self._add_empty_col_to_set(data_holder)

        for (source_name,), df in data_holder.data.group_by(self.source_col):
            try:
                aphia_id = taxa_worms.get_aphia_id(str(source_name))
            except Exception as e:
                self._log(
                    f"Could not find aphia_id for species {source_name}: {e}",
                    item=source_name,
                    level=adm_logger.WARNING,
                )
                continue
            if not aphia_id:
                self._log(
                    f"No aphia_id found for species {source_name}",
                    item=source_name,
                    level=adm_logger.WARNING,
                )
                continue
            self._add_to_col_to_set(data_holder, source_name, aphia_id)

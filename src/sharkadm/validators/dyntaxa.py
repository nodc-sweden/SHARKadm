import polars as pl

from ..data import PolarsDataHolder
from ..sharkadm_logger import adm_logger
from .base import Validator

nodc_dyntaxa = None
try:
    import nodc_dyntaxa
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class ValidateScientificNameInDyntaxa(Validator):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    col_to_check = "dyntaxa_scientific_name"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_validator_description() -> str:
        return (
            f"Checks if species in "
            f"{ValidateScientificNameInDyntaxa.col_to_check} are in "
            f"dyntaxa (taxon.csv)"
        )

    def _validate(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_dyntaxa:
            adm_logger.log_validation(
                "Could not check name in dyntaxa. "
                "Package nodc-dyntaxa not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        if self.col_to_check not in data_holder.data.columns:
            adm_logger.log_validation(
                f"Could not check name in dyntaxa. Missing column {self.col_to_check}.",
                level=adm_logger.ERROR,
            )
            return
        dyntaxa_taxon = nodc_dyntaxa.get_dyntaxa_taxon_object()
        mapper = dict((name, name) for name in dyntaxa_taxon.get_name_list())

        df = data_holder.data.with_columns(
            pl.col(self.col_to_check).replace_strict(mapper, default="").alias("mapped")
        ).filter(pl.col("mapped") == "")
        for (name,), d in df.group_by(self.col_to_check):
            if not name:
                adm_logger.log_validation("Empty scientific name")
            else:
                adm_logger.log_validation(
                    f"{name} is not a valid scientific name "
                    f"for dyntaxa ({len(d)} places)",
                    level=adm_logger.WARNING,
                )

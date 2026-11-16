import csv

import polars as pl

from sharkadm.sharkadm_operator import OperatorInfo

from ..data import DPSTemplateDataHolder
from ..sharkadm_logger import adm_logger
from .base import Validator

nodc_codes = None
try:
    import nodc_codes
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.DEBUG,
    )


class ValidateListValues(Validator):
    valid_data_holders = ("DPSTemplateDataHolder",)
    col_to_check = ""

    @classmethod
    def get_validator_description(cls) -> str:
        return (
            f"Validate that the values in column '{cls.col_to_check}' belong to "
            "the list of valid values."
        )

    def _get_valid_values(self, data_holder: DPSTemplateDataHolder):
        template_list = next(
            (
                temp_col
                for temp_col, int_col in data_holder.mapped_columns.items()
                if int_col == self.col_to_check
            ),
            None,
        )
        df = pl.read_excel(data_holder._template_path, sheet_name="Värdelistor")

        return df[template_list].drop_nulls().cast(pl.String).to_list()

    def _validate(self, data_holder: DPSTemplateDataHolder) -> OperatorInfo:
        valid_values = self._get_valid_values(data_holder)

        invalid_values = (
            data_holder.data.select(
                pl.col(self.col_to_check)
                .str.split(",")
                .list.eval(pl.element().str.strip_chars())
                .alias(self.col_to_check)
            )
            .explode(self.col_to_check)
            .filter(
                pl.col(self.col_to_check).is_not_null()
                & (pl.col(self.col_to_check) != "")
                & ~pl.col(self.col_to_check).is_in(valid_values)
            )
            .select(self.col_to_check)
            .unique()
            .to_series()
            .to_list()
        )

        has_null_values = data_holder.data.select(
            pl.col(self.col_to_check).is_null().any()
        ).item()

        has_empty_values = data_holder.data.select(
            (pl.col(self.col_to_check).str.strip_chars() == "").any()
        ).item()

        data_holder.update_checked_validation_lists(self.col_to_check)

        if invalid_values:
            invalid_values_formatted = "\n- ".join(invalid_values)
            msg = (
                f"Invalid values found in column '{self.col_to_check}'. "
                f"The following values are not included in the corresponding "
                f"validation list contains the following values:\n"
                f"- {invalid_values_formatted}"
            )
            self._log_fail(msg)
        elif has_null_values or has_empty_values:
            msg = (
                f"Missing values in column '{self.col_to_check}'. "
                "Values should come from a validation list"
            )
            self._log_fail(msg)
        else:
            msg = f"All values in column '{self.col_to_check}' are valid."
            self._log_success(msg)
        return OperatorInfo(msg=msg)


class ValidateMonitoringProgramCode(ValidateListValues):
    col_to_check = "monitoring_program_code"


class ValidateMonitoringPurposeCode(ValidateListValues):
    col_to_check = "monitoring_purpose_code"


class ValidateDataOrigin(ValidateListValues):
    col_to_check = "data_origin"


class ValidatePositioningSystemCode(ValidateListValues):
    col_to_check = "positioning_system_code"


class ValidateIceObservationCode(ValidateListValues):
    col_to_check = "ice_observation_code"


class ValidateSamplerTypeCode(ValidateListValues):
    col_to_check = "sampler_type_code"


class ValidateSpeciesFlagCode(ValidateListValues):
    col_to_check = "species_flag_code"


class ValidateTrophicTypeCode(ValidateListValues):
    col_to_check = "trophic_type_code"


class ValidatePreservationMethodCode(ValidateListValues):
    col_to_check = "preservation_method_code"


class ValidateQualityFlag(ValidateListValues):
    col_to_check = "quality_flag"


class ValidateAnalysisMethodCode(ValidateListValues):
    col_to_check = "analysis_method_code"


class ValidateCounterProgram(ValidateListValues):
    col_to_check = "counter_program"


class ValidatePlatformCode(ValidateListValues):
    col_to_check = "platform_code"

    def _get_valid_values(self, data_holder: DPSTemplateDataHolder):
        path = data_holder.config.get_path("ices_shipc.csv")
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            ship_codes = [row["Code"] for row in reader]
        return ship_codes


class ValidateSamplingLaboratoryCode(ValidateListValues):
    col_to_check = "sampling_laboratory_code"

    def _get_valid_values(self, data_holder: DPSTemplateDataHolder):
        return nodc_codes.get_labo_list(data_holder.config)


class ValidateSampleOrdererCode(ValidateListValues):
    col_to_check = "sample_orderer_code"

    def _get_valid_values(self, data_holder: DPSTemplateDataHolder):
        return nodc_codes.get_labo_list(data_holder.config)


class ValidateAnalyticalLaboratoryCode(ValidateListValues):
    col_to_check = "analytical_laboratory_code"

    def _get_valid_values(self, data_holder: DPSTemplateDataHolder):
        return nodc_codes.get_labo_list(data_holder.config)


class ValidateAllValidationListColumns(Validator):
    valid_data_holders = ("DPSTemplateDataHolder",)

    @staticmethod
    def get_validator_description() -> str:
        return (
            "Check that all columns listed as 'Värdelistor' (validation lists) have "
            "been validated against the corresponding validation list."
        )

    def _validate(self, data_holder: DPSTemplateDataHolder) -> OperatorInfo:
        checked_cols = data_holder.checked_validation_list_columns
        cols = data_holder.validation_list_columns

        # Columns listed as värdelista in template, but have not been marked as checked
        unchecked_cols = set(cols) - set(checked_cols)

        # Column marked as checked, but not listed as värdelist by template
        missing_cols = set(checked_cols) - set(cols)

        if unchecked_cols:
            unchecked_cols_formatted = "\n- ".join(unchecked_cols)
            msg = (
                f"Unvalidated columns: The following columns contain values that have "
                "not been validated against their corresponding validation lists:\n"
                f"- {unchecked_cols_formatted}"
            )
            self._log_fail(msg)

        elif missing_cols:
            missing_cols_formatted = "\n- ".join(missing_cols)
            msg = (
                f"Missing validation columns. The following columns, required for "
                f"validation against the corresponding value lists, were not found "
                f"in the data. Could this be due to an outdated template?\n"
                f"- {missing_cols_formatted}"
            )
            self._log_fail(msg)

        else:
            msg = "All columns with validation lists have been validated."
            self._log_success(msg)

        return OperatorInfo(msg=msg)

from sharkadm import validators

from .base import MultiValidator


class DpsTemplateValidation(MultiValidator):
    _validators = (
        validators.ValidateSizeClassRefListCode,
        validators.ValidateAnalysisMethodCode,
        validators.ValidateAnalyticalLaboratoryCode,
        validators.ValidateCounterProgram,
        validators.ValidateDataOrigin,
        validators.ValidateIceObservationCode,
        validators.ValidateMonitoringProgramCode,
        validators.ValidateMonitoringPurposeCode,
        validators.ValidatePlatformCode,
        validators.ValidatePositioningSystemCode,
        validators.ValidatePreservationMethodCode,
        validators.ValidateQualityFlag,
        validators.ValidateSampleOrdererCode,
        validators.ValidateSamplerTypeCode,
        validators.ValidateSamplingLaboratoryCode,
        validators.ValidateSpeciesFlagCode,
        validators.ValidateTrophicTypeCode,
        validators.ValidateAllValidationListColumns,
    )

    @staticmethod
    def get_validator_description() -> str:
        return "Performs all validators related to the DPS Template."

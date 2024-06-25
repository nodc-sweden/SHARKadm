from .base import MultiTransformer
from sharkadm import transformers


class GeneralDV(MultiTransformer):
    transformers = [
        transformers.WideToLong(),

        transformers.AddDeliveryNoteInfo(),
        transformers.AddAnalyseInfo(),
        transformers.AddSamplingInfo(),

        transformers.AddSampleMinAndMaxDepth(),
        transformers.ReorderSampleMinAndMaxDepth(),

        transformers.AddStationInfo(),

        transformers.AddSwedishProjectName(),
        transformers.AddSwedishSampleOrderer(),
        transformers.AddSwedishSamplingLaboratory(),
        transformers.AddSwedishAnalyticalLaboratory(),
        transformers.AddSwedishReportingInstitute(),

        transformers.AddEnglishProjectName(),
        transformers.AddEnglishSampleOrderer(),
        transformers.AddEnglishSamplingLaboratory(),
        transformers.AddEnglishAnalyticalLaboratory(),
        transformers.AddEnglishReportingInstitute(),

        transformers.AddStaticInternetAccessInfo(),
        transformers.AddStaticDataHoldingCenter(),

        transformers.AddLocationCounty(),
        transformers.AddLocationHelcomOsparArea(),
        transformers.AddLocationMunicipality(),
        transformers.AddLocationNation(),
        transformers.AddLocationSeaBasin(),
        transformers.AddLocationTypeArea(),
        transformers.AddLocationWaterDistrict(),

        transformers.Multiply(),
        transformers.Divide(),

        transformers.AddDatasetName(),
        transformers.FixYesNo(),

        transformers.AddColumnViewsColumns(),
        transformers.AddSharkId(),
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return f'Multi transformers that performs transformations related to Datavärdskapet.'

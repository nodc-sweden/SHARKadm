from .base import MultiTransformer
from sharkadm import transformers
from .dyntaxa import Dyntaxa


class GeneralDV(MultiTransformer):
    transformers = [
        transformers.WideToLong(),

        transformers.AddDeliveryNoteInfo(),
        transformers.AddAnalyseInfo(),
        transformers.AddSamplingInfo(),

        # transformers.AddSampleMinAndMaxDepth(),
        # transformers.ReorderSampleMinAndMaxDepth(),

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

        Dyntaxa(),

        # transformers.SetBacteriaAsReportedScientificName(),
        # transformers.AddReportedScientificName(),
        # transformers.AddTranslatedDyntaxaScientificName(),
        # transformers.AddScientificNameFromDyntaxaTranslatedScientificName(),
        # transformers.AddDyntaxaId(),
        # transformers.AddAphiaId(),

        transformers.AddDatasetName(),
        transformers.FixYesNo(),


        # transformers.AddColumnViewsColumns(),
        transformers.AddSharkId(),

        # Temp
        transformers.SortColumn(),
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return f'Multi transformers that performs transformations related to Datavärdskapet.'

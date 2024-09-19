from .base import MultiTransformer
from sharkadm import transformers
from .dyntaxa import Dyntaxa
from .location import Location
from .translate import Translate


class GeneralDV(MultiTransformer):
    transformers = [
        transformers.WideToLong,

        transformers.AddDeliveryNoteInfo,
        transformers.AddAnalyseInfo,
        transformers.AddSamplingInfo,

        # transformers.AddSampleMinAndMaxDepth,
        # transformers.ReorderSampleMinAndMaxDepth,

        transformers.AddStationInfo,

        Translate,

        transformers.AddStaticInternetAccessInfo,
        transformers.AddStaticDataHoldingCenter,

        Location,

        transformers.Multiply,
        transformers.Divide,

        Dyntaxa,

        # transformers.SetBacteriaAsReportedScientificName,
        # transformers.AddReportedScientificName,
        # transformers.AddTranslatedDyntaxaScientificName,
        # transformers.AddScientificNameFromDyntaxaTranslatedScientificName,
        # transformers.AddDyntaxaId,
        # transformers.AddAphiaId,

        transformers.AddDatasetName,
        transformers.FixYesNo,


        # transformers.AddColumnViewsColumns,
        transformers.AddSharkId,

        # Temp
        transformers.SortColumn,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs transformations related to Datavärdskapet.']
        for trans in GeneralDV.transformers:
            string_list.append(f'    {trans.name}')
        return '\n'.join(string_list)
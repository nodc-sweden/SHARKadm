from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer
from sharkadm.multi_transformers.dyntaxa import Dyntaxa
from sharkadm.multi_transformers.location import Location
from sharkadm.multi_transformers.translate import Translate


class GeneralDV(MultiTransformer):
    _transformers = (
        transformers.WideToLong,
        transformers.AddDeliveryNoteInfo,
        transformers.AddAnalyseInfo,
        transformers.AddSamplingInfo,
        # transformers.AddSampleMinAndMaxDepth,
        # transformers.ReorderSampleMinAndMaxDepth,
        transformers.AddStationInfo,
        Translate,
        transformers.AddStaticInternetAccessInfo,
        transformers.AddStaticDataHoldingCenterSwedish,
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
        transformers.SortColumns,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs transformations related to Datavärdskapet."]
        for trans in GeneralDV._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

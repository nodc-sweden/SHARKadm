from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer
from sharkadm.multi_transformers.dyntaxa import DyntaxaPolars
from sharkadm.multi_transformers.location import LocationPolars
from sharkadm.multi_transformers.translate import TranslatePolars


class GeneralDVPolars(MultiTransformer):
    _transformers = (
        transformers.PolarsWideToLong,
        transformers.PolarsAddDeliveryNoteInfo,
        transformers.PolarsAddAnalyseInfo,
        transformers.PolarsAddSamplingInfo,
        # transformers.PolarsAddSampleMinAndMaxDepth,
        # transformers.PolarsReorderSampleMinAndMaxDepth,
        # transformers.PolarsAddStationInfo,
        TranslatePolars,
        transformers.PolarsAddStaticInternetAccessInfo,
        transformers.PolarsAddStaticDataHoldingCenterSwedish,
        LocationPolars,
        transformers.PolarsMultiply,
        transformers.PolarsDivide,
        DyntaxaPolars,
        # transformers.PolarsSetBacteriaAsReportedScientificName,
        # transformers.PolarsAddReportedScientificName,
        # transformers.PolarsAddTranslatedDyntaxaScientificName,
        # transformers.PolarsAddScientificNameFromDyntaxaTranslatedScientificName,
        # transformers.PolarsAddDyntaxaId,
        # transformers.PolarsAddAphiaId,
        transformers.PolarsAddDatasetName,
        transformers.PolarsFixYesNo,
        # transformers.PolarsAddColumnViewsColumns,
        transformers.PolarsAddSharkId,
        # Temp
        transformers.PolarsSortColumns,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs transformations related to Datavärdskapet."]
        for trans in GeneralDVPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

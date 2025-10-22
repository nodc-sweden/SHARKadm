from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class TranslatePolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddSwedishProjectName,
        transformers.PolarsAddSwedishSampleOrderer,
        transformers.PolarsAddSwedishSamplingLaboratory,
        transformers.PolarsAddSwedishAnalyticalLaboratory,
        transformers.PolarsAddSwedishReportingInstitute,
        transformers.PolarsAddEnglishProjectName,
        transformers.PolarsAddEnglishSampleOrderer,
        transformers.PolarsAddEnglishSamplingLaboratory,
        transformers.PolarsAddEnglishAnalyticalLaboratory,
        transformers.PolarsAddEnglishReportingInstitute,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to translations."]
        for trans in TranslatePolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

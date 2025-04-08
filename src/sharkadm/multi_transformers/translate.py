from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class Translate(MultiTransformer):
    _transformers = (
        transformers.AddSwedishProjectName,
        transformers.AddSwedishSampleOrderer,
        transformers.AddSwedishSamplingLaboratory,
        transformers.AddSwedishAnalyticalLaboratory,
        transformers.AddSwedishReportingInstitute,
        transformers.AddEnglishProjectName,
        transformers.AddEnglishSampleOrderer,
        transformers.AddEnglishSamplingLaboratory,
        transformers.AddEnglishAnalyticalLaboratory,
        transformers.AddEnglishReportingInstitute,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to translations."]
        for trans in Translate._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

from .base import MultiTransformer
from sharkadm import transformers


class Translate(MultiTransformer):
    transformers = [
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
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to translations.']
        for trans in Translate.transformers:
            string_list.append(f'    {trans.get_transformer_description()}')
        return '\n'.join(string_list)

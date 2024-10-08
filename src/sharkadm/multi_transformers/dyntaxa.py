from .base import MultiTransformer
from sharkadm import transformers


class Dyntaxa(MultiTransformer):
    transformers = [
        transformers.MoveDyntaxaIdInReportedScientificNameToDyntaxaId,
        transformers.AddReportedDyntaxaId,
        transformers.AddTranslatedDyntaxaScientificName,
        transformers.AddDyntaxaId,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to Dyntaxa.']
        for trans in Dyntaxa.transformers:
            string_list.append(f'    {trans.name}')
        return '\n'.join(string_list)

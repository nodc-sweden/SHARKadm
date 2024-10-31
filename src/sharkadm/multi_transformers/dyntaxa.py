from .base import MultiTransformer
from sharkadm import transformers


class Dyntaxa(MultiTransformer):
    _transformers = [
        transformers.AddReportedDyntaxaId,
        transformers.AddReportedScientificNameDyntaxaId,
        transformers.AddDyntaxaScientificName,
        transformers.AddDyntaxaTranslatedScientificNameDyntaxaId,

        transformers.AddTaxonRanks,
        transformers.AddDyntaxaId,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to Dyntaxa.']
        for trans in Dyntaxa._transformers:
            string_list.append(f'    {trans.get_transformer_description()}')
        return '\n'.join(string_list)

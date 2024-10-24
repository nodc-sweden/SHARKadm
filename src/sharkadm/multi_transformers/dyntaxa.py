from .base import MultiTransformer
from sharkadm import transformers


class Dyntaxa(MultiTransformer):
    _transformers = [
        transformers.AddDyntaxaId,
        transformers.AddDyntaxaScientificName,
        transformers.AddDyntaxaScientificNameDyntaxaId,
        transformers.AddReportedDyntaxaId,
        transformers.AddReportedScientificNameDyntaxaId,
        transformers.AddTaxonRanks,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to Dyntaxa.']
        for trans in Dyntaxa._transformers:
            string_list.append(f'    {trans.get_transformer_description()}')
        return '\n'.join(string_list)

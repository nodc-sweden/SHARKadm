from .base import MultiTransformer
from sharkadm import transformers


class Worms(MultiTransformer):
    _transformers = [
        transformers.AddAphiaId,
        transformers.AddReportedAphiaId,
        transformers.AddWormsScientificName,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to Dyntaxa.']
        for trans in Worms._transformers:
            string_list.append(f'    {trans.get_transformer_description()}')
        return '\n'.join(string_list)

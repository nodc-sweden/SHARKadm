from .base import MultiTransformer
from sharkadm import transformers


class Bvol(MultiTransformer):
    _transformers = [
        transformers.AddBvolScientificNameOriginal,
        transformers.AddBvolScientificNameAndSizeClass,
        transformers.AddBvolAphiaId,
        transformers.AddBvolRefList,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to Bvol.']
        for trans in Bvol._transformers:
            string_list.append(f'    {trans.get_transformer_description()}')
        return '\n'.join(string_list)

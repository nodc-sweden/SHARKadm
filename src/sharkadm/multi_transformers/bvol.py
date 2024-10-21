from .base import MultiTransformer
from sharkadm import transformers


class Bvol(MultiTransformer):
    transformers = [
        transformers.AddBvolScientificName,
        transformers.AddBvolAphiaId,
        transformers.AddBvolSizeClass,
        transformers.AddBvolRefList,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to Bvol.']
        for trans in Bvol.transformers:
            string_list.append(f'    {trans.get_transformer_description()}')
        return '\n'.join(string_list)

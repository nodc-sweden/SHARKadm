from .base import MultiTransformer
from sharkadm import transformers


class Lims(MultiTransformer):
    transformers = [
        transformers.RemoveNonDataLines,
        transformers.MoveLessThanFlagColumnFormat,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = [f'Performs transformations related to LIMS export:']
        for trans in Lims.transformers:
            string_list.append(f'    {trans.name}')
        return '\n'.join(string_list)

from .base import MultiTransformer
from sharkadm import transformers


class Lims(MultiTransformer):
    transformers = [
        transformers.RemoveNonDataLines(),
        transformers.MoveLessThanFlagColumnFormat(),
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return f'Multi transformers that performs transformations related to LIMS export. '

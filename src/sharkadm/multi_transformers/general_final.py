from .base import MultiTransformer
from sharkadm import transformers


class GeneralFinal(MultiTransformer):
    transformers = [
        transformers.SortData(),
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return (f'Multi transformers that performs all necessary final transformations. '
                f'The idea is that this multi transformer should be applicable to all data types.')

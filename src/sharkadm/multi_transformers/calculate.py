from .base import MultiTransformer
from sharkadm import transformers


class Calculate(MultiTransformer):
    _transformers = [
        transformers.CalculateAbundance,
        transformers.CleanupCalculations,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Make calculations on data']
        for trans in Calculate._transformers:
            string_list.append(f'    {trans.get_transformer_description()}')
        return '\n'.join(string_list)

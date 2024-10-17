from .base import MultiTransformer
from sharkadm import transformers


class Position(MultiTransformer):
    transformers = [
        transformers.AddSamplePosition,
        transformers.AddSamplePositionDM,
        transformers.AddSamplePositionSweref99tm,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations needed to add station info.']
        for trans in Position.transformers:
            string_list.append(f'    {trans.get_transformer_description()}')
        return '\n'.join(string_list)

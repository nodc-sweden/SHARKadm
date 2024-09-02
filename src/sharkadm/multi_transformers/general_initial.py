from .base import MultiTransformer
from sharkadm import transformers


class GeneralInitial(MultiTransformer):
    transformers = [
        transformers.AddRowNumber,
        transformers.ReplaceCommaWithDot,
        transformers.FixTimeFormat,
        transformers.AddSampleDate,
        transformers.AddDatetime,
        transformers.AddMonth,
        transformers.AddSamplePosition,
        transformers.AddSamplePositionDM,
        transformers.AddSamplePositionSweref99tm,

        transformers.AddDatatype,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all necessary initial transformations. The idea is that this multi transformer should be applicable to all data types.']
        for trans in GeneralInitial.transformers:
            string_list.append(f'    {trans.name}')
        return '\n'.join(string_list)

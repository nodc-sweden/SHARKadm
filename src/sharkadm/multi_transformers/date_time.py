from .base import MultiTransformer
from sharkadm import transformers


class DateTime(MultiTransformer):
    transformers = [
        transformers.AddReportedDates,
        transformers.FixDateFormat,
        transformers.FixTimeFormat,
        transformers.AddSampleDate,
        transformers.AddSampleTime,
        transformers.AddDatetime,
        transformers.AddMonth,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to location.']
        for trans in DateTime.transformers:
            string_list.append(f'    {trans.name}')
        return '\n'.join(string_list)
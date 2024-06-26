from .base import MultiTransformer
from sharkadm import transformers


class GeneralInitial(MultiTransformer):
    transformers = [
        transformers.AddRowNumber(),
        transformers.ReplaceCommaWithDot(),
        transformers.FixTimeFormat(),
        transformers.AddSampleDate(),
        transformers.AddDatetime(),
        transformers.AddMonth(),
        transformers.AddSamplePosition(),
        transformers.AddSamplePositionDM(),
        transformers.AddSamplePositionSweref99tm(),

        transformers.AddDatatype(),
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return (f'Multi transformers that performs all necessary initial transformations. '
                f'The idea is that this multi transformer should be applicable to all data types.')

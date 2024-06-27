from .base import MultiTransformer
from sharkadm import transformers


class Dyntaxa(MultiTransformer):
    transformers = [
        transformers.MoveDyntaxaIdInReportedScientificNameToDyntaxaId(),
        transformers.AddReportedDyntaxaId(),
        transformers.AddTranslatedDyntaxaScientificName(),
        transformers.AddDyntaxaId(),
    ]

    @staticmethod
    def get_transformer_description() -> str:
        return (f'Multi transformers that performs all necessary final transformations. '
                f'The idea is that this multi transformer should be applicable to all data types.')

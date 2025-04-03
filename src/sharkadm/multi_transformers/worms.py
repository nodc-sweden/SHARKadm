from .base import MultiTransformer
from sharkadm import transformers


class Worms(MultiTransformer):
    _transformers = [
        transformers.AddReportedAphiaId,
        transformers.AddWormsScientificName,
        transformers.AddWormsAphiaId,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to Worms."]
        for trans in Worms._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

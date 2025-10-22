from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer, PolarsMultiTransformer


class Worms(MultiTransformer):
    _transformers = (
        transformers.AddReportedAphiaId,
        transformers.AddWormsScientificName,
        transformers.AddWormsAphiaId,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to Worms."]
        for trans in Worms._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)


class WormsPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddReportedAphiaId,
        transformers.PolarsAddWormsScientificName,
        transformers.PolarsAddWormsAphiaId,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs the following transformations related to Worms:"]
        for trans in WormsPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

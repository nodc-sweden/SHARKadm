from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class WormsPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsSetReportedAphiaIdFromAphiaId,
        transformers.PolarsAddWormsScientificName,
        transformers.PolarsAddWormsAphiaId,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs the following transformations related to Worms:"]
        for trans in WormsPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class CalculatePolars(PolarsMultiTransformer):
    _transformers = (
        # transformers.PolarsAddFloatColumn(
        #     columns=[
        #
        #     ]
        # ),
        transformers.PolarsCalculateAbundance,
        transformers.PolarsCalculateBiovolume,
        transformers.PolarsCalculateCarbon,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Make calculations on data"]
        for trans in CalculatePolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

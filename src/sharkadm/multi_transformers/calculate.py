from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class CalculatePhytoplanktonPolars(PolarsMultiTransformer):
    valid_data_types = ("phytoplankton",)
    _transformers = (
        transformers.PolarsCalculateAbundance,
        transformers.PolarsCalculateBiovolume,
        transformers.PolarsCalculateCarbon,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Make calculations on data"]
        for trans in CalculatePhytoplanktonPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

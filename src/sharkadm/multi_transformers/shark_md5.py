from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class SharkMd5Polars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddPositionDirection,
        transformers.PolarsAddPositionDDId,
        transformers.PolarsAddSharkSampleMd5,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs the following transformations needed to add Shark md5:"]
        for trans in SharkMd5Polars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

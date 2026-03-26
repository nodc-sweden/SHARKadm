from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class StaticDVPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddStaticInternetAccessInfo,
        transformers.PolarsAddStaticDataHoldingCenterEnglish,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Adds the following static information for Datavärdskapet:"]
        for trans in StaticDVPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class StaticDV(MultiTransformer):
    _transformers = (
        transformers.AddStaticInternetAccessInfo,
        transformers.AddStaticDataHoldingCenterEnglish,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Adds static information for Datavärdskapet:"]
        for trans in StaticDV._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)


class StaticDVPolars(MultiTransformer):
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

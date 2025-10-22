from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class GeneralFinal(MultiTransformer):
    _transformers = (
        transformers.StripAllValues,
        transformers.SortData,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = [
            "Performs all necessary final transformations. The idea is that this "
            "multi transformer should be applicable to all data types."
        ]
        for trans in GeneralFinal._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

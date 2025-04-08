from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class Position(MultiTransformer):
    _transformers = (
        transformers.AddSamplePositionDD,
        transformers.AddSamplePositionDM,
        transformers.AddSamplePositionSweref99tm,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations needed to add station info."]
        for trans in Position._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

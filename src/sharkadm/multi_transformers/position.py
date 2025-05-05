from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer, PolarsMultiTransformer


class Position(MultiTransformer):
    _transformers = (
        transformers.AddSamplePositionDD,
        transformers.AddSamplePositionDM,
        transformers.AddSamplePositionSweref99tm,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations needed to add position information."]
        for trans in Position._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)


class PositionPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddReportedPosition,
        transformers.PolarsAddSamplePositionDD,
        transformers.PolarsAddSamplePositionDM,
        transformers.PolarsAddSamplePositionSweref99tm,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = [
            "Performs the following transformations needed to add position information:"
        ]
        for trans in PositionPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

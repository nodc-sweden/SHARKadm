from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class GeneralInitial(MultiTransformer):
    _transformers = (
        transformers.AddRowNumber,
        transformers.ReplaceCommaWithDot,
        transformers.FixTimeFormat,
        transformers.AddSampleDate,
        transformers.AddDatetime,
        transformers.AddMonth,
        transformers.AddSamplePositionDD,
        transformers.AddSamplePositionDM,
        transformers.AddSamplePositionSweref99tm,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = [
            "Performs all necessary initial transformations. The idea is that this "
            "multi transformer should be applicable to all data types."
        ]
        for trans in GeneralInitial._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

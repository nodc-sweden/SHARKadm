from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class GeneralInitial(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddRowNumber,
        transformers.PolarsReplaceCommaWithDot,
        transformers.PolarsFixTimeFormat,
        transformers.PolarsAddSampleDate,
        transformers.PolarsAddDatetime,
        transformers.PolarsAddMonth,
        transformers.PolarsAddSamplePositionDD,
        transformers.PolarsAddSamplePositionDM,
        transformers.PolarsAddSamplePositionSweref99tm,
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

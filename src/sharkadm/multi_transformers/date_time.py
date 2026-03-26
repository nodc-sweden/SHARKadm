from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class DateTimePolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddReportedDates,
        transformers.PolarsFixDateFormat,
        transformers.PolarsFixTimeFormat,
        transformers.PolarsAddSampleDate,
        transformers.PolarsAddSampleTime,
        transformers.PolarsAddDatetime,
        transformers.PolarsAddMonth,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to time."]
        for trans in DateTimePolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

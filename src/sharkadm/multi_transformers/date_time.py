from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class DateTime(MultiTransformer):
    _transformers = (
        transformers.AddReportedDates,
        transformers.FixDateFormat,
        transformers.FixTimeFormat,
        transformers.AddSampleDate,
        transformers.AddSampleTime,
        transformers.AddDatetime,
        transformers.AddMonth,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to time."]
        for trans in DateTime._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)


class DateTimePolars(MultiTransformer):
    _transformers = (
        transformers.PolarsAddReportedDates,
        transformers.PolarsAddReportedDates,
        transformers.PolarsFixDateFormat,
        transformers.PolarsFixTimeFormat,
        transformers.PolarsAddSampleDate,
        transformers.PolarsAddSampleTime,
        transformers.PolarsAddDatetime,
        transformers.PolarsAddMonth,
    )

    # _transformers = (
    #     transformers.AddReportedDatesPolars,
    #     transformers.AddReportedDatesPolars,
    #     transformers.FixDateFormatPolars,
    #     transformers.FixTimeFormatPolars,
    #     transformers.AddSampleDatePolars,
    #     transformers.AddSampleTimePolars,
    #     transformers.AddDatetimePolars,
    #     transformers.AddMonthPolars,
    # )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to time."]
        for trans in DateTimePolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

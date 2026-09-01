from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class OccurrenceIdPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddReportedPositionString,
        transformers.AddOccurrenceId,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = [
            "Performs the following transformations needed to add occurrence_id:"
        ]
        for trans in OccurrenceIdPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

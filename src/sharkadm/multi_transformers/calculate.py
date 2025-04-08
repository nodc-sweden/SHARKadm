from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class Calculate(MultiTransformer):
    _transformers = (
        transformers.CalculateAbundance,
        transformers.CalculateBiovolume,
        transformers.CalculateCarbon,
        transformers.ReplaceNanWithEmptyString,
        transformers.RemoveReportedValueIfNotCalculated,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Make calculations on data"]
        for trans in Calculate._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

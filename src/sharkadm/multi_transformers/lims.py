from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class Lims(MultiTransformer):
    _transformers = (
        transformers.RemoveNonDataLines,
        transformers.MoveLessThanFlagColumnFormat,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs transformations related to LIMS export:"]
        for trans in Lims._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

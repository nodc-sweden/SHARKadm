from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer

# class Bvol(MultiTransformer):
#     _transformers = (
#         transformers.AddBvolScientificNameOriginal,
#         transformers.AddBvolScientificNameAndSizeClass,
#         transformers.AddBvolAphiaId,
#         transformers.AddBvolRefList,
#     )
#
#     @staticmethod
#     def get_transformer_description() -> str:
#         string_list = ["Performs all transformations related to Bvol."]
#         for trans in Bvol._transformers:
#             string_list.append(f"    {trans.get_transformer_description()}")
#         return "\n".join(string_list)


class BvolPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddBvolScientificNameOriginal,
        transformers.PolarsAddBvolScientificNameAndSizeClass,
        transformers.PolarsAddBvolAphiaId,
        transformers.PolarsAddBvolRefList,
        transformers.PolarsAddBvolCellVolume,
        transformers.PolarsAddBvolCarbonVolume,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs the following transformations related to Bvol:"]
        for trans in BvolPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

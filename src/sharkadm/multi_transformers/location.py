from sharkadm import transformers
from sharkadm.multi_transformers.base import MultiTransformer


class Location(MultiTransformer):
    _transformers = (
        transformers.AddLocationCounty,
        transformers.AddLocationHelcomOsparArea,
        transformers.AddLocationMunicipality,
        transformers.AddLocationNation,
        transformers.AddLocationSeaBasin,
        transformers.AddLocationTypeArea,
        transformers.AddLocationWaterDistrict,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to location."]
        for trans in Location._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

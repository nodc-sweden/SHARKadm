from .base import MultiTransformer
from sharkadm import transformers


class Location(MultiTransformer):
    transformers = [
        transformers.AddLocationCounty,
        transformers.AddLocationHelcomOsparArea,
        transformers.AddLocationMunicipality,
        transformers.AddLocationNation,
        transformers.AddLocationSeaBasin,
        transformers.AddLocationTypeArea,
        transformers.AddLocationWaterDistrict,
    ]

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ['Performs all transformations related to location.']
        for trans in Location.transformers:
            string_list.append(f'    {trans.name}')
        return '\n'.join(string_list)

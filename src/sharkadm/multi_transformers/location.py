from sharkadm import transformers
from sharkadm.multi_transformers.base import PolarsMultiTransformer


class LocationPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddLocationTypeArea,
        transformers.PolarsAddLocationWB,
        transformers.PolarsAddLocationTYPNFS06,
        transformers.PolarsAddLocationCounty,
        transformers.PolarsAddLocationHelcomOsparArea,
        transformers.PolarsAddLocationMunicipality,
        transformers.PolarsAddLocationNation,
        transformers.PolarsAddLocationSeaBasin,
        transformers.PolarsAddLocationWaterDistrict,
        transformers.PolarsAddLocationWaterCategory,
        transformers.PolarsAddLocationSeaAreaCode,
        transformers.PolarsAddLocationSeaAreaName,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to location."]
        for trans in LocationPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)


class LocationRPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddLocationRA,
        transformers.PolarsAddLocationRB,
        transformers.PolarsAddLocationRC,
        transformers.PolarsAddLocationRG,
        transformers.PolarsAddLocationRH,
        transformers.PolarsAddLocationRO,
        transformers.PolarsAddLocationR,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to location r."]
        for trans in LocationRPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)


class LocationRredPolars(PolarsMultiTransformer):
    _transformers = (
        transformers.PolarsAddLocationRC,
        transformers.PolarsAddLocationRG,
        transformers.PolarsAddLocationRO,
    )

    @staticmethod
    def get_transformer_description() -> str:
        string_list = ["Performs all transformations related to location r."]
        for trans in LocationRredPolars._transformers:
            string_list.append(f"    {trans.get_transformer_description()}")
        return "\n".join(string_list)

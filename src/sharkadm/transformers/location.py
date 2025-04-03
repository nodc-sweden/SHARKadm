from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

try:
    import nodc_geography
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class _AddLocationBase(Transformer):
    _cashed_data = dict()
    x_pos_col = "sample_sweref99tm_x"
    y_pos_col = "sample_sweref99tm_y"
    col_to_set = ""

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.x_pos_col not in data_holder.data.columns:
            adm_logger.log_transformation(
                f"Missing column {self.x_pos_col}. Cannot add column {self.col_to_set}",
                level=adm_logger.ERROR,
            )
            return
        if self.y_pos_col not in data_holder.data.columns:
            adm_logger.log_transformation(
                f"Missing column {self.y_pos_col}. Cannot add column {self.col_to_set}",
                level=adm_logger.ERROR,
            )
            return
        data_holder.data[self.col_to_set] = data_holder.data.apply(
            lambda row: self._get_code(row), axis=1
        )

    def _get_code(self, row):
        x_pos = row[self.x_pos_col]
        y_pos = row[self.y_pos_col]
        if not all([x_pos, y_pos]):
            return ""
        return self._cashed_data.setdefault(
            (x_pos, y_pos, self.col_to_set),
            nodc_geography.get_shape_file_info_at_position(
                x_pos=x_pos, y_pos=y_pos, variable=self.col_to_set
            )
            or "",
        )


class AddLocationWaterDistrict(_AddLocationBase):
    col_to_set = "location_water_district"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_water_district from shape files"


class AddLocationTypeArea(_AddLocationBase):
    col_to_set = "location_type_area"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_type_area from shape files"


class AddLocationSeaBasin(_AddLocationBase):
    col_to_set = "location_sea_basin"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_sea_basin from shape files"


class AddLocationNation(_AddLocationBase):
    col_to_set = "location_nation"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_nation from shape files"


class AddLocationCounty(_AddLocationBase):
    col_to_set = "location_county"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_county from shape files"


class AddLocationMunicipality(_AddLocationBase):
    col_to_set = "location_municipality"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_municipality from shape files"


class AddLocationHelcomOsparArea(_AddLocationBase):
    col_to_set = "location_helcom_ospar_area"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_helcom_ospar_area from shape files"


class AddLocationWB(_AddLocationBase):
    col_to_set = "location_wb"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_wb from shape files"


class AddLocationTYPNFS06(_AddLocationBase):
    col_to_set = "location_typ_nfs06"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_typ_nfs06 from shape files"


class AddLocationWaterCategory(Transformer):
    col_to_set = "location_water_category"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds location_water_category information"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        col = "location_typ_nfs06"
        if col not in data_holder.data:
            adm_logger.log_transformation(
                f"Missing column {col}. Cannot add column {self.col_to_set}",
                level=adm_logger.ERROR,
            )
            return

        col = "location_wb"
        if col not in data_holder.data:
            adm_logger.log_transformation(
                f"Missing column {col}. Cannot add column {self.col_to_set}",
                level=adm_logger.ERROR,
            )
            return

        boolean = data_holder.data["location_wb"] != "0"
        data_holder.data.loc[boolean, self.col_to_set] = "Havsområde innanför 1 NM"

        wb_boolean = data_holder.data["location_wb"] == "0"
        y_boolean = data_holder.data["location_typ_nfs06"] == "Y"
        p_boolean = data_holder.data["location_typ_nfs06"] == "P"
        data_holder.data.loc[wb_boolean & y_boolean, self.col_to_set] = (
            "Havsområde  mellan 1 NM och 12 NM"
        )
        data_holder.data.loc[wb_boolean & p_boolean, self.col_to_set] = (
            "Havsområde  mellan 1 NM och 12 NM"
        )

        data_holder.data.loc[wb_boolean & ~y_boolean & ~p_boolean, self.col_to_set] = (
            "Utsjövatten"
        )

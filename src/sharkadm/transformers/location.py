import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from .base import (
    DataHolderProtocol,
    PolarsDataHolderProtocol,
    PolarsTransformer,
    Transformer,
)

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
    x_pos_col = "sample_sweref99tm_x"
    y_pos_col = "sample_sweref99tm_y"
    col_to_set = ""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cached_data = {}

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.x_pos_col not in data_holder.data.columns:
            self._log(
                f"Missing column {self.x_pos_col}. Cannot add column {self.col_to_set}",
                level=adm_logger.ERROR,
            )
            return
        if self.y_pos_col not in data_holder.data.columns:
            self._log(
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
        return self._cached_data.setdefault(
            (x_pos, y_pos, self.col_to_set),
            nodc_geography.get_shape_file_info_at_position(
                x_pos=x_pos, y_pos=y_pos, variable=self.col_to_set
            )
            or "",
        )


class _PolarsAddLocationBase(PolarsTransformer):
    x_pos_col = "sample_sweref99tm_x"
    y_pos_col = "sample_sweref99tm_y"
    col_to_set = ""
    set_boolean = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cached_data = {}

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        if self.x_pos_col not in data_holder.data.columns:
            self._log(
                f"Missing column {self.x_pos_col}. Cannot add column {self.col_to_set}",
                level=adm_logger.ERROR,
            )
            return
        if self.y_pos_col not in data_holder.data.columns:
            self._log(
                f"Missing column {self.y_pos_col}. Cannot add column {self.col_to_set}",
                level=adm_logger.ERROR,
            )
            return
        if self.set_boolean:
            data_holder.data = data_holder.data.with_columns(
                pl.lit(False).alias(self.col_to_set)
            )
        else:
            data_holder.data = data_holder.data.with_columns(
                pl.lit("").alias(self.col_to_set)
            )
        for (x, y), df in data_holder.data.group_by([self.x_pos_col, self.y_pos_col]):
            code = nodc_geography.get_shape_file_info_at_position(
                x_pos=x, y_pos=y, variable=self.col_to_set
            )
            if self.set_boolean:
                code = bool(code)
                then = code
            else:
                then = pl.lit(code or "")
            data_holder.data = data_holder.data.with_columns(
                pl.when((pl.col(self.x_pos_col) == x) & (pl.col(self.y_pos_col) == y))
                .then(then)
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )


class test_PolarsAddLocations(PolarsTransformer):
    x_pos_col = "sample_sweref99tm_x"
    y_pos_col = "sample_sweref99tm_y"
    cols_to_set: tuple[str, ...] = ()
    set_boolean = False

    def __init__(self, cols_to_set: tuple[str, ...], set_boolean: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.cols_to_set = cols_to_set or self.cols_to_set
        self.set_boolean = set_boolean or self.set_boolean
        self._cached_data = {}

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        if self.x_pos_col not in data_holder.data.columns:
            self._log(
                f"Missing column {self.x_pos_col}. "
                f"Cannot add columns {', '.join(self.cols_to_set)}",
                level=adm_logger.ERROR,
            )
            return
        if self.y_pos_col not in data_holder.data.columns:
            self._log(
                f"Missing column {self.y_pos_col}. "
                f"Cannot add columns {', '.join(self.cols_to_set)}",
                level=adm_logger.ERROR,
            )
            return

        for (x, y), df in data_holder.data.group_by([self.x_pos_col, self.y_pos_col]):
            operations = []
            for col in self.cols_to_set:
                if self.set_boolean:
                    data_holder.data = data_holder.data.with_columns(
                        pl.lit(False).alias(col)
                    )
                else:
                    data_holder.data = data_holder.data.with_columns(
                        pl.lit("").alias(col)
                    )
                code = nodc_geography.get_shape_file_info_at_position(
                    x_pos=x, y_pos=y, variable=col
                )

                if self.set_boolean:
                    code = bool(code)
                    then = code
                else:
                    then = pl.lit(code or "")
                operations.append(
                    pl.when((pl.col(self.x_pos_col) == x) & (pl.col(self.y_pos_col) == y))
                    .then(then)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
            data_holder.data = data_holder.data.with_columns(operations)


class AddLocationWaterDistrict(_AddLocationBase):
    col_to_set = "location_water_district"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_water_district from shape files"


class AddLocationTypeArea(_AddLocationBase):
    col_to_set = "location_type_area"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_type_area from shape files"


class PolarsAddLocationTypeArea(_PolarsAddLocationBase):
    col_to_set = "location_type_area"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_type_area from shape files"


class AddLocationSeaBasin(_AddLocationBase):
    col_to_set = "location_sea_basin"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_sea_basin from shape files"


class AddLocationNation(_AddLocationBase):
    col_to_set = "location_nation"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_nation from shape files"


class AddLocationCounty(_AddLocationBase):
    col_to_set = "location_county"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_county from shape files"


class AddLocationMunicipality(_AddLocationBase):
    col_to_set = "location_municipality"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_municipality from shape files"


class AddLocationHelcomOsparArea(_AddLocationBase):
    col_to_set = "location_helcom_ospar_area"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_helcom_ospar_area from shape files"


class AddLocationWB(_AddLocationBase):
    col_to_set = "location_wb"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_wb from shape files"


class AddLocationTYPNFS06(_AddLocationBase):
    col_to_set = "location_typ_nfs06"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_typ_nfs06 from shape files"


class AddLocationWaterCategory(Transformer):
    col_to_set = "location_water_category"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_water_category information"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        col = "location_typ_nfs06"
        if col not in data_holder.data:
            self._log(
                f"Missing column {col}. Cannot add column {self.col_to_set}",
                level=adm_logger.ERROR,
            )
            return

        col = "location_wb"
        if col not in data_holder.data:
            self._log(
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


class PolarsAddLocationRA(_PolarsAddLocationBase):
    col_to_set = "location_ra"
    set_boolean = True

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_ra"


class PolarsAddLocationRB(_PolarsAddLocationBase):
    col_to_set = "location_rb"
    set_boolean = True

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_rb"


class PolarsAddLocationRC(_PolarsAddLocationBase):
    col_to_set = "location_rc"
    set_boolean = True

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_rc"


class PolarsAddLocationRG(_PolarsAddLocationBase):
    col_to_set = "location_rg"
    set_boolean = True

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_rg"


class PolarsAddLocationRH(_PolarsAddLocationBase):
    col_to_set = "location_rh"
    set_boolean = True

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_rh"


class PolarsAddLocationRO(_PolarsAddLocationBase):
    col_to_set = "location_ro"
    set_boolean = True

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_ro"


class PolarsAddLocationR(PolarsTransformer):
    col_to_set = "location_r"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_r"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        exp = (
            data_holder.data["location_ra"]
            | data_holder.data["location_rb"]
            | data_holder.data["location_rc"]
            | data_holder.data["location_rg"]
            | data_holder.data["location_rh"]
            | data_holder.data["location_ro"]
        )
        data_holder.data = data_holder.data.with_columns(location_r=exp)


class PolarsAddLocationWB(_PolarsAddLocationBase):
    col_to_set = "location_wb"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_wb from shape files"


class PolarsAddLocationCounty(_PolarsAddLocationBase):
    col_to_set = "location_county"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_county from shape files"

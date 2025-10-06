import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger

from .base import (
    PolarsTransformer,
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

    def _transform(self, data_holder: PolarsDataHolder) -> None:
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
        import time

        nr = 0
        t0 = time.time()
        for (x, y), df in data_holder.data.group_by([self.x_pos_col, self.y_pos_col]):
            if not nr % 100:
                # print(f"{nr}: {(time.time() - t0)=}")
                t0 = time.time()
            nr += 1
            info = nodc_geography.get_shape_file_info_at_position(
                x_pos=x, y_pos=y, variable=self.col_to_set
            )
            code = info.get(self.col_to_set, "")

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


class PolarsAddLocations(PolarsTransformer):
    x_pos_col = "sample_sweref99tm_x"
    y_pos_col = "sample_sweref99tm_y"

    def __init__(self, locations: list[str], *args, set_boolean: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self._locations = locations
        self._cached_data = {}
        self.set_boolean = set_boolean

    @staticmethod
    def get_transformer_description() -> str:
        return ""

    def _transform(self, data_holder: PolarsDataHolder) -> None:
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

        for loc in self._locations:
            if loc not in data_holder.data.columns:
                if self.set_boolean:
                    data_holder.data = data_holder.data.with_columns(
                        pl.lit(False).alias(loc)
                    )
                else:
                    data_holder.data = data_holder.data.with_columns(
                        pl.lit("").alias(loc)
                    )

        import time

        nr = 0
        t0 = time.time()
        for (x, y), df in data_holder.data.group_by([self.x_pos_col, self.y_pos_col]):
            if not nr % 100:
                # print(f"{nr}: {(time.time() - t0)=}")
                t0 = time.time()
            nr += 1
            info = dict()
            for loc in self._locations:
                if info.get(loc) is None:
                    info = nodc_geography.get_shape_file_info_at_position(
                        x_pos=x, y_pos=y, variable=loc
                    )
                code = info.get(loc, "") or ""

                if self.set_boolean:
                    code = bool(code)
                    then = code
                else:
                    then = pl.lit(code or "")
                data_holder.data = data_holder.data.with_columns(
                    pl.when((pl.col(self.x_pos_col) == x) & (pl.col(self.y_pos_col) == y))
                    .then(then)
                    .otherwise(pl.col(loc))
                    .alias(loc)
                )


class PolarsAddLocationTypeArea(_PolarsAddLocationBase):
    col_to_set = "location_type_area"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_type_area from shape files"


class PolarsAddLocationSeaAreaCode(_PolarsAddLocationBase):
    col_to_set = "location_sea_area_code"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddLocationSeaAreaCode.col_to_set} from shape files"


class PolarsAddLocationSeaAreaName(_PolarsAddLocationBase):
    col_to_set = "location_sea_area_name"

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddLocationSeaAreaCode.col_to_set} from shape files"


class PolarsAddLocationTYPNFS06(_PolarsAddLocationBase):
    col_to_set = "location_typ_nfs06"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_typ_nfs06 from shape files"


class PolarsAddLocationWaterCategory(PolarsTransformer):
    col_to_set = "location_water_category"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_water_category information"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        mandatory_cols = ("location_typ_nfs06", "location_wb")
        for col in mandatory_cols:
            if col not in data_holder.data:
                self._log(
                    f"Missing column {col}. Cannot add column {self.col_to_set}",
                    level=adm_logger.ERROR,
                )
                return

        self._add_empty_col_to_set(data_holder)

        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col("location_wb") != "0")
            .then(pl.lit("Havsområde innanför 1 NM"))
            .otherwise(pl.col(self.col_to_set))
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(
                pl.col("location_wb") == "0",
                pl.col("location_typ_nfs06") == "Y",
            )
            .then(pl.lit("Havsområde mellan 1 NM och 12 NM"))
            .otherwise(pl.col(self.col_to_set))
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(
                pl.col("location_wb") == "0",
                pl.col("location_typ_nfs06") == "P",
            )
            .then(pl.lit("Havsområde mellan 1 NM och 12 NM"))
            .otherwise(pl.col(self.col_to_set))
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(
                pl.col("location_wb") == "0",
                (pl.col("location_typ_nfs06") != "Y"),
                (pl.col("location_typ_nfs06") != "P"),
            )
            .then(pl.lit("Havsområde mellan 1 NM och 12 NM"))
            .otherwise(pl.col(self.col_to_set))
        )


class PolarsAddLocationWaterDistrict(_PolarsAddLocationBase):
    col_to_set = "location_water_district"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_water_district from shape files"


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

    def _transform(self, data_holder: PolarsDataHolder) -> None:
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


class PolarsAddLocationHelcomOsparArea(_PolarsAddLocationBase):
    col_to_set = "location_helcom_ospar_area"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_helcom_ospar_area from shape files"


class PolarsAddLocationMunicipality(_PolarsAddLocationBase):
    col_to_set = "location_municipality"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_municipality from shape files"


class PolarsAddLocationSeaBasin(_PolarsAddLocationBase):
    col_to_set = "location_sea_basin"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_sea_basin from shape files"


class PolarsAddLocationNation(_PolarsAddLocationBase):
    col_to_set = "location_nation"

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds location_nation from shape files"


class PolarsAddLocationOnLand(_PolarsAddLocationBase):
    col_to_set = "location_on_land"

    @staticmethod
    def get_transformer_description() -> str:
        return "Sets True for stations that are on land"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        col = "location_wb"
        if col not in data_holder.data:
            adm_logger.log_workflow(
                f"Could not filter data. Missing column {col}", level=adm_logger.ERROR
            )
            raise
        boolean = data_holder.data["location_wb"] == ""
        data_holder.data = data_holder.data.with_columns(
            pl.lit(boolean).alias(self.col_to_set)
        )

import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer

from ..utils import add_column

COL_COUNTED = "COPY_VARIABLE.# counted.ind/analysed sample fraction"
COL_COUNTED_REPORTED = "count_reported"
COL_COUNTED_REPORTED_FLOAT = "count_reported_float"

COL_ABUNDANCE = "COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l"
COL_ABUNDANCE_REPORTED = "abundance_reported"
COL_ABUNDANCE_REPORTED_FLOAT = "abundance_reported_float"
COL_ABUNDANCE_CALCULATED = "calculated_abundance"
COL_ABUNDANCE_COMBINED_FLOAT = "combined_abundance"

COL_BIOVOLUME = "COPY_VARIABLE.Biovolume concentration.mm3/l"
COL_BIOVOLUME_REPORTED = "biovolume_reported"
COL_BIOVOLUME_REPORTED_FLOAT = "biovolume_reported_float"
COL_BIOVOLUME_CALCULATED_FLOAT = "calculated_biovolume"
COL_BIOVOLUME_COMBINED_FLOAT = "combined_biovolume"

COL_COEFFICIENT = "coefficient"
COL_COEFFICIENT_FLOAT = "coefficient_reported"

COL_CELL_VOLUME_REPORTED = "reported_cell_volume_um3"
COL_CELL_VOLUME_REPORTED_FLOAT = "reported_cell_volume_um3_float"
# COL_CELL_VOLUME_CALCULATED = "calculated_cell_volume_um3"
COL_CELL_VOLUME_CALCULATED_FLOAT = "calculated_cell_volume_um3_float"
COL_CELL_VOLUME_COMBINED = "combined_cell_volume"

COL_CARBON = "COPY_VARIABLE.Carbon concentration.ugC/l"
COL_CARBON_REPORTED = "carbon_reported_float"
COL_CARBON_REPORTED_FLOAT = "carbon_reported_float"
COL_CARBON_PER_VOLUME = "bvol_carbon_per_volume"
COL_CARBON_PER_VOLUME_FLOAT = "bvol_carbon_per_volume_float"
# COL_CARBON_PER_VOLUME_CALCULATED = "calculated_bvol_carbon_per_volume"
COL_CARBON_PER_VOLUME_CALCULATED_FLOAT = "calculated_bvol_carbon_per_volume_float"
COL_CARBON_PER_VOLUME_COMBINED = "combined_carbon_per_volume"
COL_CARBON_CALCULATED_FLOAT = "calculated_carbon"
COL_CARBON_COMBINED = "combined_carbon"


COL_JOINED_APHIA_ID_AND_SIZE_CLASS = "aphia_id_and_size_class"

COL_CALC_BY_DC_BIOVOLUME = "calc_by_dc_biovolume"
COL_CALC_BY_DC_CARBON = "calc_by_dc_carbon"


# reported_cell_volume_col = "reported_cell_volume_um3"
# calculated_cell_volume_col = "bvol_cell_volume_um3"
# aphia_id_size_class_map_col = "aphia_id_and_size_class"
# combined_cell_volume_col = "combined_cell_volume"
# col_to_set = "calculated_biovolume"


try:
    from nodc_bvol import get_bvol_nomp_object
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f"Could not import package '{module_name}' in module {__name__}. "
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class PolarsCalculateAbundance(PolarsTransformer):
    valid_data_structures = ("column",)
    # count_col = "COPY_VARIABLE.# counted.ind/analysed sample fraction"
    # abundance_col = "COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l"
    # coef_col = "coefficient"
    # col_to_set = "calculated_abundance"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating abundance. "
            f"Setting value to column {PolarsCalculateAbundance.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(COL_ABUNDANCE).alias(COL_ABUNDANCE_REPORTED)
        )
        self._add_empty_col(data_holder, COL_COUNTED_REPORTED)

        self._add_float_columns(data_holder)

        boolean = data_holder.data[COL_COUNTED_REPORTED_FLOAT].is_not_null() & (
            data_holder.data[COL_COEFFICIENT_FLOAT].is_not_null()
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(
                (
                    pl.col(COL_COUNTED_REPORTED_FLOAT) * pl.col(COL_COEFFICIENT_FLOAT)
                ).round(1)
            )
            .otherwise(pl.lit(None))
            .cast(float)
            .alias(COL_ABUNDANCE_CALCULATED)
        )

        max_boolean = boolean & (
            data_holder.data[COL_ABUNDANCE_CALCULATED]
            > (data_holder.data[COL_ABUNDANCE_REPORTED_FLOAT] * 2)
        )

        min_boolean = boolean & (
            data_holder.data[COL_ABUNDANCE_CALCULATED]
            < (data_holder.data[COL_ABUNDANCE_REPORTED_FLOAT] * 0.5)
        )

        out_of_range_boolean = max_boolean | min_boolean

        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean)
            .then(pl.col(COL_ABUNDANCE_CALCULATED))
            .otherwise(pl.col(COL_ABUNDANCE_REPORTED_FLOAT))
            .alias(COL_ABUNDANCE_COMBINED_FLOAT),
            pl.when(out_of_range_boolean)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(""))
            .alias("calc_by_dc_abundance"),
        )

        data_holder.data = data_holder.data.with_columns(
            pl.col(COL_ABUNDANCE_COMBINED_FLOAT).alias(COL_ABUNDANCE)
        )

    def _add_float_columns(self, data_holder: PolarsDataHolder):
        data_holder.data = add_column.add_float_column(
            data_holder.data, COL_COUNTED_REPORTED, column_name=COL_COUNTED_REPORTED_FLOAT
        )
        data_holder.data = add_column.add_float_column(
            data_holder.data,
            COL_ABUNDANCE_REPORTED,
            column_name=COL_ABUNDANCE_REPORTED_FLOAT,
        )
        data_holder.data = add_column.add_float_column(
            data_holder.data, COL_COEFFICIENT, column_name=COL_COEFFICIENT_FLOAT
        )


class PolarsCalculateBiovolume(PolarsTransformer):
    valid_data_structures = ("column",)
    # bvol_col = "COPY_VARIABLE.Biovolume concentration.mm3/l"
    # abundance_col = "COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l"
    # abundance_col = "combined_abundance"
    # reported_cell_volume_col = "reported_cell_volume_um3"
    # calculated_cell_volume_col = "bvol_cell_volume_um3"
    # aphia_id_size_class_map_col = "aphia_id_and_size_class"
    # combined_cell_volume_col = "combined_cell_volume"
    # col_to_set = "calculated_biovolume"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating biovolume. "
            f"Setting value to column {COL_BIOVOLUME_CALCULATED_FLOAT}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_empty_col(data_holder, COL_BIOVOLUME)

        data_holder.data = data_holder.data.with_columns(
            pl.col(COL_BIOVOLUME).alias(COL_BIOVOLUME_REPORTED)
        )

        if COL_JOINED_APHIA_ID_AND_SIZE_CLASS not in data_holder.data:
            data_holder.data = data_holder.data.with_columns(
                pl.concat_str(
                    [
                        pl.col("bvol_aphia_id"),
                        pl.col("bvol_size_class"),
                    ],
                    separator=":",
                ).alias(COL_JOINED_APHIA_ID_AND_SIZE_CLASS)
            )

        _nomp = get_bvol_nomp_object()
        volume_mapper = _nomp.get_calculated_volume_mapper()

        data_holder.data = data_holder.data.with_columns(
            pl.col(COL_JOINED_APHIA_ID_AND_SIZE_CLASS)
            .replace_strict(volume_mapper, default=None)
            .alias(COL_CELL_VOLUME_CALCULATED_FLOAT)
            # .alias(COL_CELL_VOLUME_CALCULATED)
        )
        self._add_float_columns(data_holder)

        # Combine cell_volume. Use calculated if present else use reported
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(COL_CELL_VOLUME_CALCULATED_FLOAT).is_not_null())
            .then(pl.col(COL_CELL_VOLUME_CALCULATED_FLOAT) * 10e-9)
            .when(pl.col(COL_BIOVOLUME_REPORTED_FLOAT).is_not_null())
            .then(pl.col(COL_BIOVOLUME_REPORTED_FLOAT) * 10e-9)
            .otherwise(pl.lit(None))
            .alias(COL_CELL_VOLUME_COMBINED)
        )
        boolean = (data_holder.data[COL_ABUNDANCE_COMBINED_FLOAT].is_not_null()) & (
            data_holder.data[COL_CELL_VOLUME_COMBINED].is_not_null()
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(pl.col(COL_ABUNDANCE_COMBINED_FLOAT) * pl.col(COL_CELL_VOLUME_COMBINED))
            .otherwise(pl.lit(None))
            .alias(COL_BIOVOLUME_CALCULATED_FLOAT)
        )

        max_boolean = boolean & (
            data_holder.data[COL_BIOVOLUME_CALCULATED_FLOAT]
            > (data_holder.data[COL_BIOVOLUME_REPORTED_FLOAT] * 2)
        )

        min_boolean = boolean & (
            data_holder.data[COL_BIOVOLUME_CALCULATED_FLOAT]
            < (data_holder.data[COL_BIOVOLUME_REPORTED_FLOAT] * 0.5)
        )

        out_of_range_boolean = max_boolean | min_boolean

        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean)
            .then(pl.col(COL_BIOVOLUME_CALCULATED_FLOAT))
            .otherwise(pl.col(COL_BIOVOLUME_REPORTED_FLOAT))
            .alias(COL_BIOVOLUME_COMBINED_FLOAT),
            pl.when(out_of_range_boolean)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(""))
            .alias(COL_CALC_BY_DC_BIOVOLUME),
        )

        data_holder.data = data_holder.data.with_columns(
            pl.col(COL_BIOVOLUME_COMBINED_FLOAT).alias(COL_BIOVOLUME)
        )

    def _add_float_columns(self, data_holder: PolarsDataHolder):
        data_holder.data = add_column.add_float_column(
            data_holder.data,
            COL_BIOVOLUME_REPORTED,
            column_name=COL_BIOVOLUME_REPORTED_FLOAT,
        )


class PolarsCalculateCarbon(PolarsTransformer):
    valid_data_structures = ("column",)
    # abundance_col = "COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l"
    # carbon_col = "COPY_VARIABLE.Carbon concentration.ugC/l"
    # carbon_per_volume_col = "bvol_carbon_per_volume"

    # size_class_col = 'size_class'
    # aphia_id_col = 'aphia_id'
    # reported_cell_volume_col = 'reported_cell_volume_um3'
    # col_must_exist = 'reported_value'
    # parameter = 'Carbon concentration'
    # col_to_set = 'calculated_value'

    @staticmethod
    def get_transformer_description() -> str:
        # return (
        #     # f"Calculating {PolarsCalculateCarbon.parameter}. "
        #     # f"Setting value to column {PolarsCalculateCarbon.col_to_set}"
        # )
        return ""

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(COL_CARBON).alias(COL_CARBON_REPORTED)
        )

        _nomp = get_bvol_nomp_object()
        volume_mapper = _nomp.get_carbon_per_volume_mapper()

        data_holder.data = data_holder.data.with_columns(
            pl.col(COL_JOINED_APHIA_ID_AND_SIZE_CLASS)
            .replace_strict(volume_mapper, default=None)
            .alias(COL_CARBON_PER_VOLUME_CALCULATED_FLOAT)
        )
        self._add_float_columns(data_holder)

        # Only calculated carbon
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(COL_CARBON_PER_VOLUME_CALCULATED_FLOAT).is_not_null())
            .then(pl.col(COL_CARBON_PER_VOLUME_CALCULATED_FLOAT))
            .when(pl.col(COL_CARBON_REPORTED_FLOAT).is_not_null())
            .then(pl.col(COL_CARBON_REPORTED_FLOAT))
            .otherwise(pl.lit(None))
            .alias(COL_CARBON_PER_VOLUME_COMBINED)
        )

        boolean = (data_holder.data[COL_ABUNDANCE_COMBINED_FLOAT].is_not_null()) & (
            data_holder.data[COL_CARBON_PER_VOLUME_COMBINED].is_not_null()
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(
                pl.col(COL_ABUNDANCE_COMBINED_FLOAT)
                * pl.col(COL_CARBON_PER_VOLUME_COMBINED)
                / 1000000
            )
            .otherwise(pl.lit(None))
            .alias(COL_CARBON_CALCULATED_FLOAT)
        )

        max_boolean = boolean & (
            data_holder.data[COL_CARBON_CALCULATED_FLOAT]
            > (data_holder.data[COL_CARBON_REPORTED_FLOAT] * 2)
        )

        min_boolean = boolean & (
            data_holder.data[COL_CARBON_CALCULATED_FLOAT]
            < (data_holder.data[COL_CARBON_REPORTED_FLOAT] * 0.5)
        )

        out_of_range_boolean = max_boolean | min_boolean

        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean)
            .then(pl.col(COL_CARBON_CALCULATED_FLOAT))
            .otherwise(pl.col(COL_CARBON_REPORTED_FLOAT))
            .alias(COL_CARBON_COMBINED),
            pl.when(out_of_range_boolean)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(""))
            .alias(COL_CALC_BY_DC_CARBON),
        )

        data_holder.data = data_holder.data.with_columns(
            pl.col(COL_CARBON_COMBINED).alias(COL_CARBON)
        )

    def _add_float_columns(self, data_holder: PolarsDataHolder):
        data_holder.data = add_column.add_float_column(
            data_holder.data, COL_CARBON_REPORTED, column_name=COL_CARBON_REPORTED_FLOAT
        )

        # data_holder.data = add_column.add_float_column(
        #     data_holder.data,
        #     COL_CARBON_PER_VOLUME_CALCULATED,
        #     column_name=COL_CARBON_PER_VOLUME_CALCULATED_FLOAT
        # )

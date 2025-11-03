import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer
from sharkadm.utils import add_column

CALC_BY_DC_MARKER = "Y"

COL_CALC_BY_DC = "calc_by_dc"
COL_CELL_VOLUME_REPORTED = "reported_cell_volume_um3"

COL_PARAMETER = "parameter"
COL_PARAMETER_REPORTED = "reported_parameter"

COL_UNIT = "unit"
COL_UNIT_REPORTED = "reported_unit"

COL_VALUE = "value"
COL_VALUE_REPORTED = "reported_value"

FLOAT_COL_ABUNDANCE_CALCULATED = "abundance_calc_float"
FLOAT_COL_ABUNDANCE_COMBINED = "abundance_combined_float"
FLOAT_COL_ABUNDANCE_REPORTED = "abundance_reported_float"

FLOAT_COL_BIOVOLUME_CALCULATED = "biovolume_calc_float"
FLOAT_COL_BIOVOLUME_REPORTED = "biovolume_reported_float"
FLOAT_COL_BIOVOLUME_COMBINED = "biovolume_combined_float"

FLOAT_COL_CARBON_PER_UNIT_BVOL = "bvol_carbon_per_unit_float"
FLOAT_COL_CARBON_CALCULATED = "carbon_calc_float"
FLOAT_COL_CARBON_PER_UNIT_COMBINED = "cell_volume_um3_combined_float"
FLOAT_COL_CARBON_PER_UNIT_REPORTED = "reported_carbon_float"
FLOAT_COL_CARBON_REPORTED = "carbon_reported_float"
FLOAT_COL_CARBON_COMBINED = "carbon_combined_float"

FLOAT_COL_CELL_VOLUME_BVOL = "bvol_cell_volume_um3_float"
FLOAT_COL_CELL_VOLUME_COMBINED = "cell_volume_um3_combined_float"
FLOAT_COL_CELL_VOLUME_REPORTED = "reported_cell_volume_um3_float"

FLOAT_COL_COEFFICIENT = "coefficient_float"
FLOAT_COL_COUNTED = "# counted_float"
FLOAT_COL_VALUE = "value_float"
FLOAT_COL_VALUE_CALCULATED = "calculated_value_float"
FLOAT_COL_VALUE_REPORTED = "reported_value_float"

GIVEN_COL_ABUNDANCE = "COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l"
GIVEN_COL_BIOVOLUME = "COPY_VARIABLE.Biovolume concentration.mm3/l"
GIVEN_COL_CARBON = "COPY_VARIABLE.Carbon concentration.ugC/l"
GIVEN_COL_COUNTED = "COPY_VARIABLE.# counted.ind/analysed sample fraction"
GIVEN_COL_COEFFICIENT = "coefficient"

PAR_ABUNDANCE = "Abundance"
PAR_BIOVOLUME = "Biovolume concentration"
PAR_CARBON = "Carbon concentration"
PAR_COUNTED = "# counted"


def add_calculate_columns(data_holder: PolarsDataHolder) -> PolarsDataHolder:
    if FLOAT_COL_VALUE in data_holder.data.columns:
        return data_holder
    data_holder.data = add_column.add_float_column(
        data_holder.data, COL_VALUE, column_name=FLOAT_COL_VALUE
    )
    data_holder.data = add_column.add_float_column(
        data_holder.data, GIVEN_COL_COUNTED, column_name=FLOAT_COL_COUNTED
    )
    data_holder.data = add_column.add_float_column(
        data_holder.data, GIVEN_COL_COEFFICIENT, column_name=FLOAT_COL_COEFFICIENT
    )
    data_holder.data = add_column.add_float_column(
        data_holder.data,
        COL_CELL_VOLUME_REPORTED,
        column_name=FLOAT_COL_CELL_VOLUME_REPORTED,
    )
    data_holder.data = add_column.add_float_column(
        data_holder.data, GIVEN_COL_ABUNDANCE, column_name=FLOAT_COL_ABUNDANCE_REPORTED
    )
    data_holder.data = add_column.add_float_column(
        data_holder.data, GIVEN_COL_BIOVOLUME, column_name=FLOAT_COL_BIOVOLUME_REPORTED
    )
    data_holder.data = add_column.add_float_column(
        data_holder.data, GIVEN_COL_CARBON, column_name=FLOAT_COL_CARBON_REPORTED
    )

    data_holder.data = data_holder.data.with_columns(
        pl.col(FLOAT_COL_VALUE).alias(FLOAT_COL_VALUE_REPORTED),
        pl.lit("").alias(COL_CALC_BY_DC),
        pl.lit(None).cast(float).alias(FLOAT_COL_VALUE_CALCULATED),
        pl.lit(None).cast(float).alias(FLOAT_COL_ABUNDANCE_CALCULATED),
        pl.lit(None).cast(float).alias(FLOAT_COL_ABUNDANCE_COMBINED),
        pl.col(COL_PARAMETER).alias(COL_PARAMETER_REPORTED),
        pl.col(COL_VALUE).alias(COL_VALUE_REPORTED),
        pl.col(COL_UNIT).alias(COL_UNIT_REPORTED),
    )
    return data_holder


class PolarsCalculateAbundance(PolarsTransformer):
    valid_data_structures = ("row",)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Calculating abundance. Setting value to column {COL_VALUE}"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        try:
            add_calculate_columns(data_holder)
        except pl.exceptions.InvalidOperationError as e:
            self._log(f"Could not add calculated columns: {e}", level=adm_logger.CRITICAL)
            return
        self._calc_abundance(data_holder)
        self._add_combined_abundance(data_holder)
        self._add_to_parameter_column(data_holder)

    def _calc_abundance(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.when(self._get_not_null_boolean(data_holder))
            .then((pl.col(FLOAT_COL_COUNTED) * pl.col(FLOAT_COL_COEFFICIENT)).round(1))
            .otherwise(pl.col(FLOAT_COL_ABUNDANCE_CALCULATED))
            .alias(FLOAT_COL_ABUNDANCE_CALCULATED)
        )
        return data_holder

    def _add_combined_abundance(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.when(self._get_out_of_range_boolean(data_holder))
            .then(pl.col(FLOAT_COL_ABUNDANCE_CALCULATED))
            .otherwise(pl.col(FLOAT_COL_ABUNDANCE_REPORTED))
            .alias(FLOAT_COL_ABUNDANCE_COMBINED)
        )

    def _add_to_parameter_column(self, data_holder: PolarsDataHolder):
        out_of_range_boolean_for_par = (
            self._get_out_of_range_boolean(data_holder)
            & self._get_par_boolean(data_holder)
            # |
            # (self._get_out_of_range_boolean(data_holder) &
            #  self._get_par_boolean())
        )

        # Setting to value_float column
        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean_for_par)
            .then(pl.col(FLOAT_COL_ABUNDANCE_COMBINED))
            .otherwise(pl.col(FLOAT_COL_VALUE))
            .alias(FLOAT_COL_VALUE),
            pl.when(out_of_range_boolean_for_par)
            .then(pl.col(FLOAT_COL_ABUNDANCE_COMBINED).cast(str))
            .otherwise(pl.col(COL_VALUE))
            .alias(COL_VALUE),
            pl.when(out_of_range_boolean_for_par)
            .then(pl.lit(CALC_BY_DC_MARKER))
            .otherwise(pl.col(COL_CALC_BY_DC))
            .alias(COL_CALC_BY_DC),
        )

    def _get_par_boolean(self, data_holder: PolarsDataHolder) -> pl.Series:
        return data_holder.data["parameter"] == PAR_ABUNDANCE

    def _get_not_null_boolean(self, data_holder: PolarsDataHolder) -> pl.Series:
        not_null_boolean = (
            data_holder.data[FLOAT_COL_COUNTED].is_not_null()
            & data_holder.data[FLOAT_COL_COEFFICIENT].is_not_null()
        )
        return not_null_boolean

    def _get_out_of_range_boolean(self, data_holder: PolarsDataHolder) -> pl.Series:
        max_boolean = data_holder.data[FLOAT_COL_ABUNDANCE_CALCULATED] > (
            data_holder.data[FLOAT_COL_ABUNDANCE_REPORTED] * 2
        )

        min_boolean = data_holder.data[FLOAT_COL_ABUNDANCE_CALCULATED] < (
            data_holder.data[FLOAT_COL_ABUNDANCE_REPORTED] * 0.5
        )

        out_of_range_boolean = (max_boolean | min_boolean) & self._get_not_null_boolean(
            data_holder
        )
        return out_of_range_boolean


class PolarsCalculateBiovolume(PolarsTransformer):
    valid_data_structures = ("column",)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Calculating biovolume. Setting value to column {COL_VALUE}"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        add_calculate_columns(data_holder)
        self._add_combined_cell_volume(data_holder)
        self._calc_biovolume(data_holder)
        self._add_combined_biovolume(data_holder)
        self._add_to_parameter_column(data_holder)

    def _add_combined_cell_volume(self, data_holder: PolarsDataHolder):
        # Combine cell_volume. Use calculated by bvol if present else use reported
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(FLOAT_COL_CELL_VOLUME_BVOL).is_not_null())
            .then(pl.col(FLOAT_COL_CELL_VOLUME_BVOL))
            .otherwise(pl.col(FLOAT_COL_CELL_VOLUME_REPORTED))
            .alias(FLOAT_COL_CELL_VOLUME_COMBINED)
        )

    def _calc_biovolume(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.when(self._get_not_null_boolean(data_holder))
            .then(
                pl.col(FLOAT_COL_ABUNDANCE_COMBINED)
                * pl.col(FLOAT_COL_CELL_VOLUME_COMBINED)
            )
            .otherwise(pl.lit(None))
            .alias(FLOAT_COL_BIOVOLUME_CALCULATED)
        )
        return data_holder

    def _add_combined_biovolume(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.when(self._get_out_of_range_boolean(data_holder))
            .then(pl.col(FLOAT_COL_BIOVOLUME_CALCULATED))
            .otherwise(pl.col(FLOAT_COL_BIOVOLUME_REPORTED))
            .alias(FLOAT_COL_BIOVOLUME_COMBINED)
        )

    def _add_to_parameter_column(self, data_holder: PolarsDataHolder):
        out_of_range_boolean_for_par = (
            self._get_out_of_range_boolean(data_holder) & self._get_par_boolean()
        )

        # Setting to value_float column
        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean_for_par)
            .then(pl.col(FLOAT_COL_BIOVOLUME_COMBINED))
            .otherwise(pl.col(FLOAT_COL_VALUE))
            .alias(FLOAT_COL_VALUE),
            pl.when(out_of_range_boolean_for_par)
            .then(pl.col(FLOAT_COL_BIOVOLUME_COMBINED).cast(str))
            .otherwise(pl.col(COL_VALUE))
            .alias(COL_VALUE),
            pl.when(out_of_range_boolean_for_par)
            .then(pl.lit(CALC_BY_DC_MARKER))
            .otherwise(pl.col(COL_CALC_BY_DC))
            .alias(COL_CALC_BY_DC),
        )

    def _get_par_boolean(self) -> pl.Series:
        return pl.col("parameter") == PAR_BIOVOLUME

    def _get_not_null_boolean(self, data_holder: PolarsDataHolder) -> pl.Series:
        not_null_boolean = (
            data_holder.data[FLOAT_COL_ABUNDANCE_COMBINED].is_not_null()
        ) & (data_holder.data[FLOAT_COL_CELL_VOLUME_COMBINED].is_not_null())
        return not_null_boolean

    def _get_out_of_range_boolean(self, data_holder: PolarsDataHolder) -> pl.Series:
        max_boolean = data_holder.data[FLOAT_COL_BIOVOLUME_CALCULATED] > (
            data_holder.data[FLOAT_COL_BIOVOLUME_REPORTED] * 2
        )

        min_boolean = data_holder.data[FLOAT_COL_BIOVOLUME_CALCULATED] < (
            data_holder.data[FLOAT_COL_BIOVOLUME_REPORTED] * 0.5
        )

        out_of_range_boolean = (max_boolean | min_boolean) & self._get_not_null_boolean(
            data_holder
        )
        return out_of_range_boolean


class PolarsCalculateCarbon(PolarsTransformer):
    valid_data_structures = ("column",)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Calculating carbon. Setting value to column {COL_VALUE}"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        add_calculate_columns(data_holder)
        self._calc_carbon(data_holder)
        self._add_combined_carbon(data_holder)
        self._add_to_parameter_column(data_holder)

    def _calc_carbon(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.when(self._get_not_null_boolean(data_holder))
            .then(
                pl.col(FLOAT_COL_ABUNDANCE_COMBINED)
                * pl.col(FLOAT_COL_CARBON_PER_UNIT_BVOL)
                / 1_000_000
            )
            .otherwise(pl.lit(None))
            .alias(FLOAT_COL_CARBON_CALCULATED)
        )

    def _add_combined_carbon(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.when(self._get_out_of_range_boolean(data_holder))
            .then(pl.col(FLOAT_COL_CARBON_CALCULATED))
            .otherwise(pl.col(FLOAT_COL_CARBON_REPORTED))
            .alias(FLOAT_COL_CARBON_COMBINED)
        )

    def _add_to_parameter_column(self, data_holder: PolarsDataHolder):
        out_of_range_boolean_for_par = (
            self._get_out_of_range_boolean(data_holder) & self._get_par_boolean()
        )

        # Setting to value_float column
        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean_for_par)
            .then(pl.col(FLOAT_COL_CARBON_COMBINED))
            .otherwise(pl.col(FLOAT_COL_VALUE))
            .alias(FLOAT_COL_VALUE),
            pl.when(out_of_range_boolean_for_par)
            .then(pl.col(FLOAT_COL_CARBON_COMBINED).cast(str))
            .otherwise(pl.col(COL_VALUE))
            .alias(COL_VALUE),
            pl.when(out_of_range_boolean_for_par)
            .then(pl.lit(CALC_BY_DC_MARKER))
            .otherwise(pl.col(COL_CALC_BY_DC))
            .alias(COL_CALC_BY_DC),
        )

    def _get_par_boolean(self) -> pl.Series:
        return pl.col("parameter") == PAR_CARBON

    def _get_not_null_boolean(self, data_holder: PolarsDataHolder) -> pl.Series:
        not_null_boolean = (
            data_holder.data[FLOAT_COL_ABUNDANCE_COMBINED].is_not_null()
        ) & (data_holder.data[FLOAT_COL_CARBON_PER_UNIT_COMBINED].is_not_null())
        return not_null_boolean

    def _get_out_of_range_boolean(self, data_holder: PolarsDataHolder) -> pl.Series:
        max_boolean = data_holder.data[FLOAT_COL_CARBON_CALCULATED] > (
            data_holder.data[FLOAT_COL_CARBON_REPORTED] * 2
        )

        min_boolean = data_holder.data[FLOAT_COL_CARBON_CALCULATED] < (
            data_holder.data[FLOAT_COL_CARBON_REPORTED] * 0.5
        )

        out_of_range_boolean = (max_boolean | min_boolean) & self._get_not_null_boolean(
            data_holder
        )
        return out_of_range_boolean


class PolarsOnlyKeepReportedIfCalcByDc(PolarsTransformer):
    valid_data_structures = ("row",)

    @staticmethod
    def get_transformer_description() -> str:
        return "Removes values in reported_-columns if not calculated by dc"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if COL_CALC_BY_DC not in data_holder.data.columns:
            self._log(f"No column named {COL_CALC_BY_DC} in data!",
                      level=adm_logger.ERROR)
            return
        boolean = pl.col(COL_CALC_BY_DC) != CALC_BY_DC_MARKER
        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(pl.lit(""))
            .otherwise(pl.col(COL_PARAMETER_REPORTED))
            .alias(COL_PARAMETER_REPORTED),
            pl.when(boolean)
            .then(pl.lit(""))
            .otherwise(pl.col(COL_VALUE_REPORTED))
            .alias(COL_VALUE_REPORTED),
            pl.when(boolean)
            .then(pl.lit(""))
            .otherwise(pl.col(COL_UNIT_REPORTED))
            .alias(COL_UNIT_REPORTED),
        )


class PolarsFixCalcByDc(PolarsTransformer):
    valid_data_structures = ("row",)
    col_to_set = "calc_by_dc"
    check_columns = (
        ("Abundance", "calc_by_dc_abundance"),
        ("Biovolume concentration", "calc_by_dc_biovolume"),
        ("Carbon concentration", "calc_by_dc_carbon"),
    )

    @staticmethod
    def get_transformer_description() -> str:
        return "Arranging calc_by_dc from calculated variables"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_empty_col_to_set(data_holder)
        for par, source_col in self.check_columns:
            boolean = (pl.col(source_col) == CALC_BY_DC_MARKER) & (
                pl.col("parameter") == par
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(boolean)
                .then(pl.lit(CALC_BY_DC_MARKER))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set),
                pl.when(boolean)
                .then(pl.lit(par))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set),
            )

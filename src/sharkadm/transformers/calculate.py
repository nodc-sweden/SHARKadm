import polars as pl

from sharkadm.data import PolarsDataHolder
from sharkadm.sharkadm_logger import adm_logger
from sharkadm.transformers.base import PolarsTransformer

from ..utils import add_column

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
    count_col = "COPY_VARIABLE.# counted.ind/analysed sample fraction"
    abundance_col = "COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l"
    coef_col = "coefficient"
    col_to_set = "calculated_abundance"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating abundance. "
            f"Setting value to column {PolarsCalculateAbundance.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_float_columns(data_holder)
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.abundance_col).alias("reported_abundance")
        )
        self._add_empty_col_to_set(data_holder)

        boolean = data_holder.data["count_float"].is_not_null() & (
            data_holder.data["coef_float"].is_not_null()
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then((
                pl.col("count_float")
                * pl.col("coef_float")).round(1)
            )
            .otherwise(pl.lit(None))
            .cast(float)
            .alias(self.col_to_set)
        )

        max_boolean = boolean & (data_holder.data[self.col_to_set] > (
            data_holder.data["abundance_float"] * 2
        ))

        min_boolean = boolean & (data_holder.data[self.col_to_set] < (
            data_holder.data["abundance_float"] * 0.5
        ))

        out_of_range_boolean = max_boolean | min_boolean

        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean)
            .then(pl.col(self.col_to_set))
            .otherwise(pl.col(self.abundance_col))
            .alias("combined_abundance"),
            pl.when(out_of_range_boolean)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(""))
            .alias("calc_by_dc_abundance"),
        )

    def _add_float_columns(self, data_holder: PolarsDataHolder):
        data_holder.data = add_column.add_float_column(
            data_holder.data,
            self.count_col,
            column_name="count_float"
        )
        data_holder.data = add_column.add_float_column(
            data_holder.data,
            self.coef_col,
            column_name="coef_float"
        )
        data_holder.data = add_column.add_float_column(
            data_holder.data,
            self.abundance_col,
            column_name="abundance_float"
        )


class PolarsCalculateBiovolume(PolarsTransformer):
    valid_data_structures = ("column",)
    bvol_col = "COPY_VARIABLE.Biovolume concentration.mm3/l"
    # abundance_col = "COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l"
    abundance_col = "combined_abundance"
    reported_cell_volume_col = "reported_cell_volume_um3"
    calculated_cell_volume_col = "bvol_cell_volume_um3"
    aphia_id_size_class_map_col = "aphia_id_and_size_class"
    combined_cell_volume_col = "combined_cell_volume"
    col_to_set = "calculated_biovolume"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Calculating biovolume. "
            f"Setting value to column {PolarsCalculateBiovolume.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.bvol_col not in data_holder.data.columns:
            data_holder.data = data_holder.data.with_columns(
                pl.lit("").alias(self.bvol_col)
            )
        # data_holder.data = data_holder.data.with_columns(
        #     pl.col(self.bvol_col).alias(self.reported_cell_volume_col)
        # )

        self._add_empty_col_to_set(data_holder)

        if self.aphia_id_size_class_map_col not in data_holder.data:
            data_holder.data = data_holder.data.with_columns(
                pl.concat_str(
                    [
                        pl.col("bvol_aphia_id"),
                        pl.col("bvol_size_class"),
                    ],
                    separator=":",
                ).alias(self.aphia_id_size_class_map_col)
            )

        _nomp = get_bvol_nomp_object()
        volume_mapper = _nomp.get_calculated_volume_mapper()

        data_holder.data = data_holder.data.with_columns(
            pl.col(self.aphia_id_size_class_map_col)
            .replace_strict(volume_mapper, default="")
            .alias("temp_calculated_cell_volume")
        )
        self._add_float_columns(data_holder)

        # Combine cell_volume. Use calculated if present else use reported
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(self.calculated_cell_volume_col).is_not_null())
            .then(pl.col(self.calculated_cell_volume_col).cast(float) * 10e-9)
            .when(pl.col(self.reported_cell_volume_col).is_not_null())
            .then(pl.col(self.reported_cell_volume_col).cast(float) * 10e-9)
            .otherwise(pl.lit(None))
            .alias(self.combined_cell_volume_col)
        )
        boolean = (data_holder.data["combined_abundance_float"].is_not_null()) & (
            data_holder.data[self.combined_cell_volume_col].is_not_null()
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(
                pl.col("combined_abundance_float").cast(float)
                * pl.col(self.combined_cell_volume_col).cast(float)
            )
            .otherwise(pl.lit(None))
            .alias(self.col_to_set)
        )

        max_boolean = boolean & (data_holder.data[self.col_to_set].cast(float) > (
            (data_holder.data["bvol_float"].cast(float) * 2))
        )

        min_boolean = boolean & (data_holder.data[self.col_to_set].cast(float) < (
            (data_holder.data["bvol_float"].cast(float) * 0.5))
        )

        out_of_range_boolean = max_boolean | min_boolean

        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean)
            .then(pl.col(self.col_to_set))
            .otherwise(pl.col("bvol_float").cast(str))
            .alias("combined_biovolume"),
            pl.when(out_of_range_boolean)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(""))
            .alias("calc_by_dc_biovolume"),
        )

    def _add_float_columns(self, data_holder: PolarsDataHolder):
        data_holder.data = add_column.add_float_column(
            data_holder.data,
            self.bvol_col,
            column_name="bvol_float"
        )

        data_holder.data = add_column.add_float_column(
            data_holder.data,
            "temp_calculated_cell_volume",
            column_name="calculated_cell_volume_float"
        )

        data_holder.data = add_column.add_float_column(
            data_holder.data,
            "combined_abundance",
            column_name="combined_abundance_float"
        )


class PolarsCalculateCarbon(PolarsTransformer):
    valid_data_structures = ("column",)
    abundance_col = "COPY_VARIABLE.Abundance.ind/l or 100 um pieces/l"
    carbon_col = "COPY_VARIABLE.Carbon concentration.ugC/l"
    carbon_per_volume_col = "bvol_carbon_per_volume"

    # size_class_col = 'size_class'
    # aphia_id_col = 'aphia_id'
    # reported_cell_volume_col = 'reported_cell_volume_um3'
    # col_must_exist = 'reported_value'
    # parameter = 'Carbon concentration'
    # col_to_set = 'calculated_value'

    @staticmethod
    def get_transformer_description() -> str:
        return (
            # f"Calculating {PolarsCalculateCarbon.parameter}. "
            # f"Setting value to column {PolarsCalculateCarbon.col_to_set}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.carbon_col)
            .alias("reported_carbon")
        )



        bool_carbon = data_holder.data[self.carbon_per_volume_col] != ""

        data_holder.data = data_holder.data.with_columns(
            pl.when(bool_carbon)
            .then(
                pl.col(self.abundance_col).cast(float)
                * pl.col(self.carbon_per_volume_col).cast(float)
            )
            .otherwise(pl.lit(""))
            .alias(self.col_to_set)
        )

        max_boolean = bool_carbon & (data_holder.data[self.col_to_set].cast(float) > (
            (data_holder.data[self.carbon_col].cast(float) * 2))
                                 )

        min_boolean = bool_carbon & (data_holder.data[self.col_to_set].cast(float) < (
            (data_holder.data[self.carbon_col].cast(float) * 0.5))
                                 )

        out_of_range_boolean = max_boolean | min_boolean

        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean)
            .then(pl.col(self.col_to_set))
            .otherwise(pl.col(self.carbon_col))
            .alias("combined_carbon"),
            pl.when(out_of_range_boolean)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(""))
            .alias("calc_by_dc_biovolume"),
        )





        data_holder.data["calculated_carbon"] = data_holder.data.loc[
            bool_carbon, self.abundance_col
        ].astype(float) * data_holder.data.loc[
            bool_carbon, self.carbon_per_volume_col
        ].astype(float)

        max_boolean = data_holder.data.loc[bool_carbon, "calculated_carbon"] > (
            data_holder.data.loc[bool_carbon, self.carbon_col].astype(float) * 2
        )
        min_boolean = data_holder.data.loc[bool_carbon, "calculated_carbon"] < (
            data_holder.data.loc[bool_carbon, self.carbon_col].astype(float) * 0.5
        )
        out_of_range_boolean = max_boolean | min_boolean
        for i, row in data_holder.data[out_of_range_boolean].iterrows():
            # Log here
            pass
        data_holder.data.loc[out_of_range_boolean, self.abundance_col] = (
            data_holder.data.loc[out_of_range_boolean, "calculated_carbon"]
        )
        data_holder.data.loc[out_of_range_boolean, "calc_by_dc_carbon"] = "Y"
        data_holder.data["carbon_unit"] = "ugC/l"

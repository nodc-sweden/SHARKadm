import polars as pl

from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from .base import DataHolderProtocol, PolarsTransformer, Transformer

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
        return (f"Calculating abundance. "
                f"Setting value to column {PolarsCalculateAbundance.col_to_set}")

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.abundance_col).alias("reported_abundance")
        )
        self._add_empty_col_to_set(data_holder)

        boolean = (data_holder.data[self.count_col] != "") & (
            data_holder.data[self.coef_col] != ""
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(
                pl.col(self.count_col).cast(float)
                * pl.col(self.coef_col).cast(float).round(1)
            )
            .otherwise(pl.lit(""))
            .alias(self.col_to_set)
        )

        max_boolean = boolean & data_holder.data[self.col_to_set].cast(float) > (
            data_holder.data[self.abundance_col].cast(float) * 2
        )

        min_boolean = boolean & data_holder.data[self.col_to_set].cast(float) < (
            data_holder.data[self.abundance_col].cast(float) * 0.5
        )

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


class PolarsCalculateBiovolume(PolarsTransformer):
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
        return (f"Calculating biovolume. "
                f"Setting value to column {PolarsCalculateBiovolume.col_to_set}")

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.bvol_col).alias("reported_cell_volume_um3")
        )
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
            .alias(self.calculated_cell_volume_col)
        )

        # Combine cell_volume. Use calculated if present else use reported
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(self.calculated_cell_volume_col) != "")
            .then(pl.col(self.calculated_cell_volume_col).cast(float) * 10e-9)
            .when(pl.col(self.reported_cell_volume_col) != "")
            .then(pl.col(self.reported_cell_volume_col).cast(float) * 10e-9)
            .otherwise(pl.lit(""))
            .alias(self.combined_cell_volume_col)
        )

        boolean = (data_holder.data[self.abundance_col] != "") & (
            data_holder.data[self.combined_cell_volume_col] != ""
        )

        data_holder.data = data_holder.data.with_columns(
            pl.when(boolean)
            .then(
                pl.col(self.abundance_col).cast(float)
                * pl.col(self.combined_cell_volume_col).cast(float)
            )
            .otherwise(pl.lit(""))
            .alias(self.col_to_set)
        )

        max_boolean = boolean & data_holder.data[self.col_to_set].cast(float) > (
            data_holder.data[self.bvol_col].cast(float) * 2
        )

        min_boolean = boolean & data_holder.data[self.col_to_set].cast(float) < (
            data_holder.data[self.bvol_col].cast(float) * 0.5
        )

        out_of_range_boolean = max_boolean | min_boolean

        data_holder.data = data_holder.data.with_columns(
            pl.when(out_of_range_boolean)
            .then(pl.col(self.col_to_set))
            .otherwise(pl.col(self.bvol_col))
            .alias("combined_biovolume"),
            pl.when(out_of_range_boolean)
            .then(pl.lit("Y"))
            .otherwise(pl.lit(""))
            .alias("calc_by_dc_biovolume"),
        )


class PolarsCalculateCarbon(Transformer):
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
        return (f"Calculating {PolarsCalculateCarbon.parameter}. "
                f"Setting value to column {PolarsCalculateCarbon.col_to_set}")

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data["reported_carbon"] = data_holder.data[self.carbon_col]
        bool_carbon = data_holder.data[self.carbon_per_volume_col] != ""

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

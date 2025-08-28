import polars as pl

from ..data import PolarsDataHolder
from ..sharkadm_logger import adm_logger
from .base import (
    PolarsTransformer,
)

try:
    import nodc_bvol
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class PolarsAddBvolScientificNameOriginal(PolarsTransformer):
    valid_data_types = ("Phytoplankton",)
    source_col = "reported_scientific_name"
    col_to_set = "bvol_scientific_name_original"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddBvolScientificNameOriginal.col_to_set} "
            f"from {PolarsAddBvolScientificNameOriginal.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_column(data_holder)
        # self._log_result(data_holder)

    def _add_column(self, data_holder: PolarsDataHolder):
        translate_bvol_name = nodc_bvol.get_translate_bvol_name_object()
        _mapper = translate_bvol_name.get_scientific_name_from_to_mapper()

        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col)
            .replace_strict(_mapper, default=pl.col(self.source_col))
            .alias(self.col_to_set)
        )

        for (from_name, to_name), df in data_holder.data.filter(
            pl.col(self.source_col) != pl.col(self.col_to_set)
        ).group_by(self.source_col, self.col_to_set):
            self._log(
                f"Translating {from_name} -> {to_name} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            # TODO: Log level here?
            #  Maybe just log reported_scientific_name -> final scientific_name

    def _log_result(self, data_holder: PolarsDataHolder):
        for (from_name, to_name), df in data_holder.data.filter(
            pl.col(self.source_col) != pl.col(self.col_to_set)
        ).group_by(self.source_col, self.col_to_set):
            self._log(
                f"Translating to Bvol scientific name: "
                f"{from_name} -> {to_name} ({len(df)} places)",
                level=adm_logger.INFO,
            )


class PolarsAddBvolScientificNameAndSizeClass(PolarsTransformer):
    valid_data_types = ("Phytoplankton",)

    source_name_col = "bvol_scientific_name_original"
    source_size_class_col = "size_class"
    col_to_set_name = "bvol_scientific_name"
    col_to_set_size = "bvol_size_class"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddBvolScientificNameAndSizeClass.col_to_set_name} "
            f"and {PolarsAddBvolScientificNameAndSizeClass.col_to_set_size}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_column(data_holder)
        # self._log_result(data_holder)

    def _add_column(self, data_holder: PolarsDataHolder):
        if self.source_size_class_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_size_class_col} "
                f"when setting bvol information",
                level=adm_logger.Warning,
            )
            return

        self._remove_columns(data_holder, self.col_to_set_name, self.col_to_set_size)

        translate_bvol_name_and_size = nodc_bvol.get_translate_bvol_name_size_object()
        _mapper = translate_bvol_name_and_size.get_scientific_name_from_to_mapper()

        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(
                [pl.col(self.source_name_col), pl.col(self.source_size_class_col)],
                separator=":",
            ).alias("bvol_combined_from"),
        )

        data_holder.data = data_holder.data.with_columns(
            pl.col("bvol_combined_from")
            .replace_strict(_mapper, default=pl.col("bvol_combined_from"))
            .alias("bvol_combined_to")
        )

        data_holder.data = data_holder.data.with_columns(
            pl.col("bvol_combined_to")
            .str.split_exact(":", 1)
            .struct.rename_fields([self.col_to_set_name, self.col_to_set_size])
            .alias("fields")
        ).unnest("fields")

    def _log_result(self, data_holder: PolarsDataHolder):
        for (from_name, to_name), df in data_holder.data.filter(
            pl.col("bvol_combined_from") != pl.col("bvol_combined_to")
        ).group_by("bvol_combined_from", "bvol_combined_to"):
            self._log(
                f"Translating Bvol name and size_class "
                f"{from_name} -> {to_name} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            # TODO: Log level here?
            #  Maybe just log reported_scientific_name -> final scientific_name


class PolarsAddBvolAphiaId(PolarsTransformer):
    valid_data_types = ("Phytoplankton",)

    scientific_name_col = "bvol_scientific_name"
    size_class_col = "bvol_size_class"
    col_to_set = "bvol_aphia_id"
    joined_col = f"{scientific_name_col}:{size_class_col}"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddBvolAphiaId.col_to_set} "
            f"from {PolarsAddBvolAphiaId.scientific_name_col} and "
            f"{PolarsAddBvolAphiaId.size_class_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.scientific_name_col not in data_holder.data:
            self._log(
                f"Missing column {self.scientific_name_col} "
                f"when trying to set bvol aphia_id",
                level=adm_logger.WARNING,
            )
            return
        if self.size_class_col not in data_holder.data:
            self._log(
                f"Missing column {self.size_class_col} when trying to set bvol aphia_id",
                level=adm_logger.WARNING,
            )
            return
        self._add_joined_column(data_holder)
        self._add_column(data_holder)
        # self._log_result(data_holder)

    def _add_joined_column(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(
                [
                    pl.col(self.scientific_name_col),
                    pl.col(self.size_class_col),
                ],
                separator=":",
            ).alias(self.joined_col)
        )

    def _add_column(self, data_holder: PolarsDataHolder):
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        _mapper = bvol_nomp.get_species_to_aphia_id_mapper()
        # TODO: One species might map to multiple aphia_ids. Check this!
        # Species = 1138
        # AphiaID = 1133
        # Kombination = 1136

        data_holder.data = data_holder.data.with_columns(
            pl.col(self.joined_col)
            .replace_strict(_mapper, default="")
            .alias(self.col_to_set)
        )

    # def _log_result(self, data_holder: PolarsDataHolder):
    #     for (from_name, to_name), df in data_holder.data.filter(
    #         pl.col(self.source_col) != pl.col(self.col_to_set)
    #     ).group_by(self.source_col, self.col_to_set):
    #         self._log(
    #             f"Adding Bvol AphiaID: {from_name} -> {to_name} ({len(df)} places)",
    #             level=adm_logger.INFO,
    #         )
    #         # TODO: Log level here?
    #         #  Maybe just log reported_scientific_name -> final scientific_name


class PolarsAddBvolRefList(PolarsTransformer):
    valid_data_types = ("Phytoplankton",)

    scientific_name_col = "bvol_scientific_name"
    size_class_col = "bvol_size_class"
    col_to_set = "bvol_ref_list"
    joined_col = f"{scientific_name_col}:{size_class_col}"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddBvolRefList.col_to_set} "
            f"from {PolarsAddBvolRefList.scientific_name_col} and "
            f"{PolarsAddBvolRefList.size_class_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.scientific_name_col not in data_holder.data:
            self._log(
                f"Missing column {self.scientific_name_col} "
                f"when trying to set bvol ref list",
                level=adm_logger.WARNING,
            )
            return
        if self.size_class_col not in data_holder.data:
            self._log(
                f"Missing column {self.size_class_col} when trying to set bvol ref list",
                level=adm_logger.WARNING,
            )
            return

        self._add_joined_column(data_holder)
        self._add_column(data_holder)
        # self._log_result(data_holder)

    def _add_joined_column(self, data_holder: PolarsDataHolder):
        data_holder.data = data_holder.data.with_columns(
            pl.concat_str(
                [
                    pl.col(self.scientific_name_col),
                    pl.col(self.size_class_col),
                ],
                separator=":",
            ).alias(self.joined_col)
        )

    def _add_column(self, data_holder: PolarsDataHolder):
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        _mapper = bvol_nomp.get_species_to_ref_list_mapper()
        # TODO: One species might map to multiple aphia_ids. Check this! Might be fixed...
        # Species = 1138
        # AphiaID = 1133
        # Kombination = 1136

        data_holder.data = data_holder.data.with_columns(
            pl.col(self.joined_col)
            .replace_strict(_mapper, default="")
            .alias(self.col_to_set)
        )

    # def _log_result(self, data_holder: PolarsDataHolder):
    #     for (from_name, to_name), df in data_holder.data.filter(
    #         pl.col(self.scientific_name_col) != pl.col(self.col_to_set)
    #     ).group_by(self.scientific_name_col, self.col_to_set):
    #         self._log(
    #             f"Adding Bvol ref list: {from_name} -> {to_name} ({len(df)} places)",
    #             level=adm_logger.INFO,
    #         )

import polars as pl

from ..data import PolarsDataHolder
from ..sharkadm_logger import adm_logger
from .base import (
    DataHolderProtocol,
    PolarsDataHolderProtocol,
    PolarsTransformer,
    Transformer,
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


class AddBvolScientificNameOriginal(Transformer):
    valid_data_types = ("Phytoplankton", "IFCB")
    source_col = "reported_scientific_name"
    col_to_set = "bvol_scientific_name_original"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddBvolScientificNameOriginal.col_to_set} "
            f"from {AddBvolScientificNameOriginal.source_col}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        translate_bvol_name = nodc_bvol.get_translate_bvol_name_object()
        for name, df in data_holder.data.groupby(self.source_col):
            new_name = translate_bvol_name.get(str(name))
            if new_name:
                self._log(
                    f"Translating {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            else:
                self._log(
                    f"Adding {name} to {self.col_to_set} ({len(df)} places)",
                    level=adm_logger.DEBUG,
                )
                new_name = name

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = new_name


class AddBvolScientificNameAndSizeClass(Transformer):
    valid_data_types = ("Phytoplankton", "IFCB")

    source_name_col = "bvol_scientific_name_original"
    source_size_class_col = "size_class"
    col_to_set_name = "bvol_scientific_name"
    col_to_set_size = "bvol_size_class"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddBvolScientificNameAndSizeClass.col_to_set_name} "
            f"and {AddBvolScientificNameAndSizeClass.col_to_set_size}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_size_class_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_size_class_col} "
                f"when setting bvol information. Will search without size_class",
                level=adm_logger.DEBUG,
            )
            self._transform_without_size_class(data_holder)
        else:
            self._transform_with_size_class(data_holder)

    def _transform_with_size_class(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set_name] = ""
        data_holder.data[self.col_to_set_size] = ""
        translate_bvol_name_and_size = nodc_bvol.get_translate_bvol_name_size_object()
        for (name, size), df in data_holder.data.groupby(
            [self.source_name_col, self.source_size_class_col]
        ):
            info = translate_bvol_name_and_size.get(name, size)
            new_name = info.get("name") or name
            new_size_class = info.get("size_class") or size
            if new_name != name:
                self._log(
                    f"Translate bvol name: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            data_holder.data.loc[df.index, self.col_to_set_name] = new_name
            if new_size_class != size:
                self._log(
                    f"Translate bvol size_class: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            data_holder.data.loc[df.index, self.col_to_set_size] = new_size_class

    def _transform_without_size_class(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set_name] = ""
        data_holder.data[self.col_to_set_size] = ""
        translate_bvol_name_and_size = nodc_bvol.get_translate_bvol_name_size_object()
        for name, df in data_holder.data.groupby(self.source_name_col):
            info = translate_bvol_name_and_size.get(str(name))
            new_name = info.get("name") or name
            if new_name != name:
                self._log(
                    f"Translate bvol name: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            data_holder.data.loc[df.index, self.col_to_set_name] = new_name


class AddBvolRefList(Transformer):
    valid_data_types = ("Phytoplankton", "IFCB")

    source_col = "bvol_scientific_name"
    col_to_set = "bvol_ref_list"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {AddBvolRefList.col_to_set} from {AddBvolRefList.source_col}"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_col} when trying to set bvol ref list",
                level=adm_logger.WARNING,
            )
            return
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        data_holder.data[self.col_to_set] = ""
        for name, df in data_holder.data.groupby(self.source_col):
            lst = bvol_nomp.get_info(Species=name)
            if not lst:
                continue
            if isinstance(lst, list):
                text = ", ".join(
                    sorted(set([item["List"] for item in lst if item["List"]]))
                )
            else:
                text = lst["List"]
            self._log(
                f"Setting {self.col_to_set} for {name}: {text} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            data_holder.data.loc[df.index, self.col_to_set] = text


class AddBvolAphiaId(Transformer):
    valid_data_types = ("Phytoplankton", "IFCB")

    source_col = "bvol_scientific_name"
    col_to_set = "bvol_aphia_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {AddBvolAphiaId.col_to_set} from {AddBvolAphiaId.source_col}"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_col} when trying to set bvol aphia_id",
                level=adm_logger.WARNING,
            )
            return
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        data_holder.data[self.col_to_set] = ""
        for name, df in data_holder.data.groupby(self.source_col):
            lst = bvol_nomp.get_info(Species=name)
            if not lst:
                continue
            if isinstance(lst, list):
                text = ", ".join(
                    sorted(set([item["AphiaID"] for item in lst if item["AphiaID"]]))
                )
            else:
                text = lst["AphiaID"]
            self._log(
                f"Setting {self.col_to_set} for {name}: {text} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            data_holder.data.loc[df.index, self.col_to_set] = text


class OldPolarsAddBvolScientificNameOriginal(PolarsTransformer):
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

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set)
        )
        translate_bvol_name = nodc_bvol.get_translate_bvol_name_object()
        for (name,), df in data_holder.data.group_by(self.source_col):
            new_name = translate_bvol_name.get(str(name))
            if new_name:
                self._log(
                    f"Translating {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            else:
                self._log(
                    f"Adding {name} to {self.col_to_set} ({len(df)} places)",
                    level=adm_logger.DEBUG,
                )
                new_name = name
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_col) == name)
                .then(pl.lit(new_name))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
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
                f"when setting bvol information. Will search without size_class",
                level=adm_logger.DEBUG,
            )
            self.source_size_class_col = "_temp_size_class"
            self._add_empty_col(data_holder, self.source_size_class_col)

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


class OldPolarsAddBvolScientificNameAndSizeClass(PolarsTransformer):
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

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set_name)
        )
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set_size)
        )
        if self.source_size_class_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_size_class_col} "
                f"when setting bvol information. Will search without size_class",
                level=adm_logger.DEBUG,
            )
            self._transform_without_size_class(data_holder)
        else:
            self._transform_with_size_class(data_holder)

    def _transform_with_size_class(self, data_holder: PolarsDataHolderProtocol) -> None:
        translate_bvol_name_and_size = nodc_bvol.get_translate_bvol_name_size_object()
        for (name, size), df in data_holder.data.group_by(
            [self.source_name_col, self.source_size_class_col]
        ):
            info = translate_bvol_name_and_size.get(str(name), str(size))
            new_name = info.get("name") or name
            new_size_class = info.get("size_class") or size
            if new_name != name:
                self._log(
                    f"Translate bvol name: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )

            data_holder.data = data_holder.data.with_columns(
                pl.when(
                    pl.col(self.source_name_col) == name,
                    pl.col(self.source_size_class_col) == size,
                )
                .then(pl.lit(new_name))
                .otherwise(pl.col(self.col_to_set_name))
                .alias(self.col_to_set_name)
            )
            if new_size_class != size:
                self._log(
                    f"Translate bvol size_class: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )

            data_holder.data = data_holder.data.with_columns(
                pl.when(
                    pl.col(self.source_name_col) == name,
                    pl.col(self.source_size_class_col) == size,
                )
                .then(pl.lit(new_size_class))
                .otherwise(pl.col(self.col_to_set_size))
                .alias(self.col_to_set_size)
            )

    def _transform_without_size_class(
        self, data_holder: PolarsDataHolderProtocol
    ) -> None:
        translate_bvol_name_and_size = nodc_bvol.get_translate_bvol_name_size_object()
        for (name,), df in data_holder.data.group_by(self.source_name_col):
            info = translate_bvol_name_and_size.get(str(name))
            new_name = info.get("name") or name
            if new_name != name:
                self._log(
                    f"Translate bvol name: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_name_col) == name)
                .then(pl.lit(new_name))
                .otherwise(pl.col(self.col_to_set_name))
                .alias(self.col_to_set_name)
            )


class OldPolarsAddBvolAphiaId(Transformer):
    valid_data_types = ("Phytoplankton",)

    source_col = "bvol_scientific_name"
    col_to_set = "bvol_aphia_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddBvolAphiaId.col_to_set} "
            f"from {PolarsAddBvolAphiaId.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_col} when trying to set bvol aphia_id",
                level=adm_logger.WARNING,
            )
            return
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set)
        )
        for (name,), df in data_holder.data.group_by(self.source_col):
            lst = bvol_nomp.get_info(Species=name)
            if not lst:
                continue
            if isinstance(lst, list):
                text = ", ".join(
                    sorted(set([item["AphiaID"] for item in lst if item["AphiaID"]]))
                )
            else:
                text = lst["AphiaID"]
            self._log(
                f"Setting {self.col_to_set} for {name}: {text} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_col) == name)
                .then(pl.lit(text))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )


class PolarsAddBvolAphiaId(PolarsTransformer):
    valid_data_types = ("Phytoplankton",)

    source_col = "bvol_scientific_name"
    col_to_set = "bvol_aphia_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddBvolAphiaId.col_to_set} "
            f"from {PolarsAddBvolAphiaId.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_col} when trying to set bvol aphia_id",
                level=adm_logger.WARNING,
            )
            return
        self._add_column(data_holder)
        # self._log_result(data_holder)

    def _add_column(self, data_holder: PolarsDataHolder):
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        _mapper = bvol_nomp.get_species_to_aphia_id_mapper()
        # TODO: One species might map to multiple aphia_ids. Check this!
        # Species = 1138
        # AphiaID = 1133
        # Kombination = 1136

        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col)
            .replace_strict(_mapper, default="")
            .alias(self.col_to_set)
        )

    def _log_result(self, data_holder: PolarsDataHolder):
        for (from_name, to_name), df in data_holder.data.filter(
            pl.col(self.source_col) != pl.col(self.col_to_set)
        ).group_by(self.source_col, self.col_to_set):
            self._log(
                f"Adding Bvol AphiaID: {from_name} -> {to_name} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            # TODO: Log level here?
            #  Maybe just log reported_scientific_name -> final scientific_name


class PolarsAddBvolRefList(PolarsTransformer):
    valid_data_types = ("Phytoplankton",)

    source_col = "bvol_scientific_name"
    col_to_set = "bvol_ref_list"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddBvolRefList.col_to_set} "
            f"from {PolarsAddBvolRefList.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_col} when trying to set bvol ref list",
                level=adm_logger.WARNING,
            )
            return

        self._add_column(data_holder)
        # self._log_result(data_holder)

    def _add_column(self, data_holder: PolarsDataHolder):
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        _mapper = bvol_nomp.get_species_to_ref_list_mapper()
        # TODO: One species might map to multiple aphia_ids. Check this!
        # Species = 1138
        # AphiaID = 1133
        # Kombination = 1136

        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col)
            .replace_strict(_mapper, default="")
            .alias(self.col_to_set)
        )

    def _log_result(self, data_holder: PolarsDataHolder):
        for (from_name, to_name), df in data_holder.data.filter(
            pl.col(self.source_col) != pl.col(self.col_to_set)
        ).group_by(self.source_col, self.col_to_set):
            self._log(
                f"Adding Bvol ref list: {from_name} -> {to_name} ({len(df)} places)",
                level=adm_logger.INFO,
            )


class OldPolarsAddBvolRefList(PolarsTransformer):
    valid_data_types = ("Phytoplankton",)

    source_col = "bvol_scientific_name"
    col_to_set = "bvol_ref_list"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddBvolRefList.col_to_set} "
            f"from {PolarsAddBvolRefList.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            self._log(
                f"Missing column {self.source_col} when trying to set bvol ref list",
                level=adm_logger.WARNING,
            )
            return
        bvol_nomp = nodc_bvol.get_bvol_nomp_object()
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set)
        )
        for (name,), df in data_holder.data.group_by(self.source_col):
            lst = bvol_nomp.get_info(Species=name)
            if not lst:
                continue
            if isinstance(lst, list):
                text = ", ".join(
                    sorted(set([item["List"] for item in lst if item["List"]]))
                )
            else:
                text = lst["List"]
            self._log(
                f"Setting {self.col_to_set} for {name}: {text} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_col) == name)
                .then(pl.lit(text))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )


class NewPolarsAddBvolInfo(PolarsTransformer):
    valid_data_types = ("Phytoplankton",)
    col_reported_scientific_name = "reported_scientific_name"
    col_bvol_scientific_name_original = "bvol_scientific_name_original"

    # source_col = "reported_scientific_name"
    # col_to_set = "bvol_scientific_name_original"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        # UPPDATERA
        return (
            f"Adds {PolarsAddBvolScientificNameOriginal.col_to_set} "
            f"from {PolarsAddBvolScientificNameOriginal.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        translate_bvol_name = nodc_bvol.get_translate_bvol_name_object()
        self._add_empty_columns(data_holder)

        for (reported_name,), df in data_holder.data.group_by(
            self.col_reported_scientific_name
        ):
            bvol_scientific_name_original = translate_bvol_name.get(str(reported_name))
            if bvol_scientific_name_original:
                self._log(
                    f"Translating {reported_name} -> {bvol_scientific_name_original} "
                    f"({len(df)} places)",
                    level=adm_logger.INFO,
                )
            else:
                self._log(
                    f"Adding {reported_name} to {self.col_to_set} ({len(df)} places)",
                    level=adm_logger.DEBUG,
                )
                bvol_scientific_name_original = reported_name

            self._add_to_col(
                data_holder,
                self.col_reported_scientific_name,
                self.col_bvol_scientific_name_original,
                reported_name,
                bvol_scientific_name_original,
            )

    def _add_empty_columns(self, data_holder: PolarsDataHolder):
        self._add_empty_col(data_holder, self.col_bvol_scientific_name_original)

    def _add_to_col(
        self,
        data_holder: PolarsDataHolder,
        source_col,
        col_to_set,
        lookup_name,
        new_name: str,
    ) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.when(pl.col(source_col) == lookup_name)
            .then(pl.lit(new_name))
            .otherwise(pl.col(col_to_set))
            .alias(col_to_set)
        )

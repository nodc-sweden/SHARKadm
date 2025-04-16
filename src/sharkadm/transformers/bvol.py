from ..sharkadm_logger import adm_logger
from .base import DataHolderProtocol, Transformer, PolarsDataHolderProtocol, \
    PolarsTransformer
import polars as pl

try:
    import nodc_bvol

    bvol_nomp = nodc_bvol.get_bvol_nomp_object()
    translate_bvol_name = nodc_bvol.get_translate_bvol_name_object()
    translate_bvol_name_and_size = nodc_bvol.get_translate_bvol_name_size_object()
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
        for name, df in data_holder.data.groupby(self.source_col):
            new_name = translate_bvol_name.get(str(name))
            if new_name:
                adm_logger.log_transformation(
                    f"Translating {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            else:
                adm_logger.log_transformation(
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
            adm_logger.log_transformation(
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
        for (name, size), df in data_holder.data.groupby(
            [self.source_name_col, self.source_size_class_col]
        ):
            info = translate_bvol_name_and_size.get(name, size)
            new_name = info.get("name") or name
            new_size_class = info.get("size_class") or size
            if new_name != name:
                adm_logger.log_transformation(
                    f"Translate bvol name: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            data_holder.data.loc[df.index, self.col_to_set_name] = new_name
            if new_size_class != size:
                adm_logger.log_transformation(
                    f"Translate bvol size_class: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            data_holder.data.loc[df.index, self.col_to_set_size] = new_size_class

    def _transform_without_size_class(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set_name] = ""
        data_holder.data[self.col_to_set_size] = ""
        for name, df in data_holder.data.groupby(self.source_name_col):
            info = translate_bvol_name_and_size.get(str(name))
            new_name = info.get("name") or name
            if new_name != name:
                adm_logger.log_transformation(
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
            adm_logger.log_transformation(
                f"Missing column {self.source_col} when trying to set bvol ref list",
                level=adm_logger.WARNING,
            )
            return
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
            adm_logger.log_transformation(
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
            adm_logger.log_transformation(
                f"Missing column {self.source_col} when trying to set bvol aphia_id",
                level=adm_logger.WARNING,
            )
            return
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
            adm_logger.log_transformation(
                f"Setting {self.col_to_set} for {name}: {text} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            data_holder.data.loc[df.index, self.col_to_set] = text


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

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set)
        )

        for (name, ), df in data_holder.data.group_by(self.source_col):
            new_name = translate_bvol_name.get(str(name))
            if new_name:
                adm_logger.log_transformation(
                    f"Translating {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            else:
                adm_logger.log_transformation(
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


class PolarsAddBvolScientificNameAndSizeClass(PolarsTransformer):
    valid_data_types = ("Phytoplankton", )

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
            adm_logger.log_transformation(
                f"Missing column {self.source_size_class_col} "
                f"when setting bvol information. Will search without size_class",
                level=adm_logger.DEBUG,
            )
            self._transform_without_size_class(data_holder)
        else:
            self._transform_with_size_class(data_holder)

    def _transform_with_size_class(self, data_holder: PolarsDataHolderProtocol) -> None:
        for (name, size), df in data_holder.data.group_by(
            [self.source_name_col, self.source_size_class_col]
        ):
            info = translate_bvol_name_and_size.get(str(name), str(size))
            new_name = info.get("name") or name
            new_size_class = info.get("size_class") or size
            if new_name != name:
                adm_logger.log_transformation(
                    f"Translate bvol name: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )

            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_name_col) == name,
                        pl.col(self.source_size_class_col) == size)
                .then(pl.lit(new_name))
                .otherwise(pl.col(self.col_to_set_name))
                .alias(self.col_to_set_name)
            )
            if new_size_class != size:
                adm_logger.log_transformation(
                    f"Translate bvol size_class: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )

            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_name_col) == name,
                        pl.col(self.source_size_class_col) == size)
                .then(pl.lit(new_size_class))
                .otherwise(pl.col(self.col_to_set_size))
                .alias(self.col_to_set_size)
            )

    def _transform_without_size_class(self, data_holder: PolarsDataHolderProtocol) -> None:
        for (name, ), df in data_holder.data.group_by(self.source_name_col):
            info = translate_bvol_name_and_size.get(str(name))
            new_name = info.get("name") or name
            if new_name != name:
                adm_logger.log_transformation(
                    f"Translate bvol name: {name} -> {new_name} ({len(df)} places)",
                    level=adm_logger.INFO,
                )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_name_col) == name)
                .then(pl.lit(new_name))
                .otherwise(pl.col(self.col_to_set_name))
                .alias(self.col_to_set_name)
            )


class PolarsAddBvolAphiaId(Transformer):
    valid_data_types = ("Phytoplankton", )

    source_col = "bvol_scientific_name"
    col_to_set = "bvol_aphia_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddBvolAphiaId.col_to_set} from {PolarsAddBvolAphiaId.source_col}"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            adm_logger.log_transformation(
                f"Missing column {self.source_col} when trying to set bvol aphia_id",
                level=adm_logger.WARNING,
            )
            return
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set)
        )
        for (name, ), df in data_holder.data.group_by(self.source_col):
            lst = bvol_nomp.get_info(Species=name)
            if not lst:
                continue
            if isinstance(lst, list):
                text = ", ".join(
                    sorted(set([item["AphiaID"] for item in lst if item["AphiaID"]]))
                )
            else:
                text = lst["AphiaID"]
            adm_logger.log_transformation(
                f"Setting {self.col_to_set} for {name}: {text} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_col) == name)
                .then(pl.lit(text))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )


class PolarsAddBvolRefList(PolarsTransformer):
    valid_data_types = ("Phytoplankton", )

    source_col = "bvol_scientific_name"
    col_to_set = "bvol_ref_list"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {PolarsAddBvolRefList.col_to_set} from {PolarsAddBvolRefList.source_col}"

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            adm_logger.log_transformation(
                f"Missing column {self.source_col} when trying to set bvol ref list",
                level=adm_logger.WARNING,
            )
            return
        data_holder.data = data_holder.data.with_columns(
            pl.lit("").alias(self.col_to_set)
        )
        for (name, ), df in data_holder.data.group_by(self.source_col):
            lst = bvol_nomp.get_info(Species=name)
            if not lst:
                continue
            if isinstance(lst, list):
                text = ", ".join(
                    sorted(set([item["List"] for item in lst if item["List"]]))
                )
            else:
                text = lst["List"]
            adm_logger.log_transformation(
                f"Setting {self.col_to_set} for {name}: {text} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            data_holder.data = data_holder.data.with_columns(
                pl.when(pl.col(self.source_col) == name)
                .then(pl.lit(text))
                .otherwise(pl.col(self.col_to_set))
                .alias(self.col_to_set)
            )
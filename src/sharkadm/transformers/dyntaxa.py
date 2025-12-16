import polars as pl

from ..data import PolarsDataHolder
from ..sharkadm_logger import adm_logger
from .base import (
    DataHolderProtocol,
    PolarsDataHolderProtocol,
    PolarsTransformer,
    Transformer,
)

nodc_dyntaxa = None
try:
    import nodc_dyntaxa

    translate_dyntaxa = nodc_dyntaxa.get_translate_dyntaxa_object()
    dyntaxa_taxon = nodc_dyntaxa.get_dyntaxa_taxon_object()
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class AddReportedDyntaxaId(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    col_to_set = "reported_dyntaxa_id"
    source_col = "dyntaxa_id"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddReportedDyntaxaId.col_to_set} "
            f"from {AddReportedDyntaxaId.source_col} if not given."
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data.columns:
            self._log(
                f"No source column {self.source_col}. "
                f"Setting empty column {self.col_to_set}",
                level=adm_logger.DEBUG,
            )
            data_holder.data[self.col_to_set] = ""
            return
        if self.col_to_set in data_holder.data.columns:
            self._log(
                f"Column {self.col_to_set} already in data. Will not add",
                level=adm_logger.DEBUG,
            )
            return

        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]


class AddDyntaxaScientificName(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "reported_scientific_name"
    col_to_set = "dyntaxa_scientific_name"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddDyntaxaScientificName.col_to_set} translated from nodc_dyntaxa. "
            f"Source column is {AddDyntaxaScientificName.source_col}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ""
        for name, df in data_holder.data.groupby(self.source_col):
            name = str(name)
            new_name = translate_dyntaxa.get(name)
            if new_name:
                if name.isdigit():
                    new_name_2 = translate_dyntaxa.get(new_name)
                    if new_name_2:
                        self._log(
                            f"Translated from dyntaxa: {name} -> {new_name} -> "
                            f"{new_name_2} ({len(df)} places)",
                            level=adm_logger.INFO,
                        )
                        new_name = new_name_2
                    else:
                        self._log(
                            f"No second translation for: {name} ({len(df)} places)",
                            level=adm_logger.DEBUG,
                        )
                else:
                    self._log(
                        f"Translated from dyntaxa: {name} -> {new_name} "
                        f"({len(df)} places)",
                        level=adm_logger.INFO,
                    )
            else:
                if name.isdigit():
                    self._log(
                        f"{self.source_col} {name} seems to be a dyntaxa_id "
                        f"and could not be translated ({len(df)} places)",
                        level=adm_logger.WARNING,
                    )
                else:
                    self._log(
                        f"No translation for: {name} ({len(df)} places)",
                        level=adm_logger.WARNING,
                    )
                new_name = name

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = new_name


class AddDyntaxaTranslatedScientificNameDyntaxaId(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "reported_scientific_name"
    col_to_set = "dyntaxa_translated_scientific_name_dyntaxa_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddDyntaxaTranslatedScientificNameDyntaxaId.col_to_set} "
            f"translated from nodc_dyntaxa. Source column is "
            f"{AddDyntaxaTranslatedScientificNameDyntaxaId.source_col}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ""
        for name, df in data_holder.data.groupby(self.source_col):
            name = str(name)
            _id = translate_dyntaxa.get_dyntaxa_id(name)
            if not _id:
                continue
            self._log(
                f"Adding {_id} to {self.col_to_set} ({len(df)} places)",
                level=adm_logger.INFO,
            )

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = _id


class AddDyntaxaId(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "dyntaxa_scientific_name"
    col_to_set = "dyntaxa_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddDyntaxaId.col_to_set} translated from nodc_dyntaxa. "
            f"Source column is {AddDyntaxaId.source_col}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data.columns:
            self._log(
                f"Could not add column {self.col_to_set}. "
                f"Source column {self.source_col} not in data.",
                level=adm_logger.ERROR,
            )
            return
        if self.col_to_set not in data_holder.data.columns:
            self._log(f"Adding empty column {self.col_to_set}", level=adm_logger.DEBUG)
            data_holder.data[self.col_to_set] = ""

        for name, df in data_holder.data.groupby(self.source_col):
            if not str(name).strip():
                self._log(
                    f"Missing {self.source_col} when trying to add dyntaxa_id, "
                    f"{len(df)} rows.",
                    level=adm_logger.WARNING,
                )
                continue
            dyntaxa_id = dyntaxa_taxon.get(str(name))
            if not dyntaxa_id:
                self._log(
                    f"No {self.col_to_set} found for {name}, {len(df)} rows.",
                    level=adm_logger.WARNING,
                )
                continue
            index = data_holder.data[self.source_col] == name
            self._log(
                f"Adding {self.col_to_set} {dyntaxa_id} translated from {name} "
                f"({len(df)} places)",
                level=adm_logger.INFO,
            )
            data_holder.data.loc[index, self.col_to_set] = dyntaxa_id


class AddReportedScientificNameDyntaxaId(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "reported_scientific_name"
    col_to_set = "reported_scientific_name_dyntaxa_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddReportedScientificNameDyntaxaId.col_to_set} "
            f"from {AddReportedScientificNameDyntaxaId.source_col} if it is a digit."
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ""
        for name, df in data_holder.data.groupby(self.source_col):
            name = str(name)
            if not name.isdigit():
                continue
            self._log(
                f"Adding dyntaxa_id {name} to {self.col_to_set} "
                f"from {self.source_col} ({len(df)} places)",
                level=adm_logger.DEBUG,
            )

            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = name


class AddTaxonRanks(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll", "bacterioplankton")
    ranks = (
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "taxon_hierarchy",
    )
    cols_to_set = tuple(
        f"taxon_{rank}" if "taxon" not in rank else rank for rank in ranks
    )
    source_col = "dyntaxa_scientific_name"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds taxon rank columns. Data from dyntaxa."

    def _add_columns(self, data_holder: DataHolderProtocol):
        for col in self.cols_to_set:
            data_holder.data[col] = ""

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        self._add_columns(data_holder=data_holder)
        for name, df in data_holder.data.groupby(self.source_col):
            info = dyntaxa_taxon.get_info(scientificName=name, taxonomicStatus="accepted")
            if not info:
                self._log(
                    f"Could not add information about taxon rank for {name} "
                    f"({len(df)} places)",
                    item=name,
                    level=adm_logger.WARNING,
                )
                continue
            if len(info) != 1:
                self._log(
                    f"Several matches in dyntaxa for {name} ({len(df)} places)",
                    item=name,
                    level=adm_logger.WARNING,
                )
                continue
            single_info = info[0]
            self._log(
                f"Adding taxon rank for {name} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            for rank, col in zip(self.ranks, self.cols_to_set):
                value = single_info.get(rank, "") or ""
                boolean = data_holder.data[self.source_col] == name
                data_holder.data.loc[boolean, col] = value
                self._log(
                    f"Adding {col} for {name} ({len(df)} places)",
                    level=adm_logger.DEBUG,
                )


class PolarsAddReportedDyntaxaId(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    col_to_set = "reported_dyntaxa_id"
    source_col = "dyntaxa_id"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddReportedDyntaxaId.col_to_set} "
            f"from {PolarsAddReportedDyntaxaId.source_col} if not given."
        )

    def _transform(self, data_holder: PolarsDataHolderProtocol) -> None:
        if self.col_to_set in data_holder.data.columns:
            self._log(
                f"Column {self.col_to_set} already in data. Will not add",
                level=adm_logger.DEBUG,
            )
            return
        if self.source_col not in data_holder.data.columns:
            self._log(
                f"No source column {self.source_col}. "
                f"Setting empty column {self.col_to_set}",
                level=adm_logger.DEBUG,
            )
            self._add_empty_col_to_set(data_holder)
            return
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )


class PolarsAddReportedScientificNameDyntaxaId(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "reported_scientific_name"
    col_to_set = "reported_scientific_name_dyntaxa_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddReportedScientificNameDyntaxaId.col_to_set} "
            f"from {PolarsAddReportedScientificNameDyntaxaId.source_col} "
            f"if it is a digit."
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        self._add_empty_col_to_set(data_holder)
        for (name,), df in data_holder.data.group_by(self.source_col):
            name = str(name)
            if not name.isdigit():
                continue
            self._log(
                f"Adding dyntaxa_id {name} to {self.col_to_set} "
                f"from {self.source_col} ({len(df)} places)",
                level=adm_logger.DEBUG,
            )
            self._add_to_col_to_set(data_holder, name, name)


class PolarsAddDyntaxaScientificName(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "reported_scientific_name"
    col_to_set = "dyntaxa_scientific_name"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddDyntaxaScientificName.col_to_set} "
            f"translated from nodc_dyntaxa. "
            f"Source column is {AddDyntaxaScientificName.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_dyntaxa:
            self._log(
                "Could not add dyntaxa scientific name. "
                "Package nodc-dyntaxa not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        self._add_empty_col_to_set(data_holder)
        for (name,), df in data_holder.data.group_by(self.source_col):
            name = str(name)
            new_name = translate_dyntaxa.get(name)
            if new_name:
                if name.isdigit():
                    new_name_2 = translate_dyntaxa.get(new_name)
                    if new_name_2:
                        self._log(
                            f"Translated using {translate_dyntaxa.source}. "
                            f"Reported name: {name} "
                            f"Translated to: {new_name_2} ({len(df)} rows)",
                            # f"Translated from dyntaxa: {name} -> {new_name} -> "
                            # f"{new_name_2} ({len(df)} places)",
                            level=adm_logger.INFO,
                        )
                        new_name = new_name_2
                    else:
                        self._log(
                            f"No second translation for: {new_name} ({len(df)} places)",
                            level=adm_logger.DEBUG,
                        )
                else:
                    self._log(
                        f"Translated using {translate_dyntaxa.source}. "
                        f"Reported name: {name} "
                        f"Translated to: {new_name} ({len(df)} rows)",
                        # f"Translated from dyntaxa: {name} -> {new_name} "
                        # f"({len(df)} places)",
                        level=adm_logger.INFO,
                    )
            else:
                if name.isdigit():
                    self._log(
                        f"{self.source_col} {name} seems to be a dyntaxa_id "
                        f"and could not be translated ({len(df)} rows)",
                        level=adm_logger.WARNING,
                    )
                else:
                    self._log(
                        f"No translation ({translate_dyntaxa.source}) for: "
                        f"{name} ({len(df)} rows)",
                        level=adm_logger.DEBUG,
                    )
                new_name = name

            self._add_to_col_to_set(data_holder, name, new_name)
            # data_holder.data = data_holder.data.with_columns(
            #     pl.when(pl.col(self.source_col) == name)
            #     .then(pl.lit(new_name))
            #     .otherwise(pl.col(self.col_to_set))
            #     .alias(self.col_to_set)
            # )


class PolarsAddDyntaxaTranslatedScientificNameDyntaxaId(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "reported_scientific_name"
    col_to_set = "dyntaxa_translated_scientific_name_dyntaxa_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {PolarsAddDyntaxaTranslatedScientificNameDyntaxaId.col_to_set} "
            f"translated from nodc_dyntaxa. Source column is "
            f"{PolarsAddDyntaxaTranslatedScientificNameDyntaxaId.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_dyntaxa:
            self._log(
                "Could not add dyntaxa scientific name. "
                "Package nodc-dyntaxa not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        self._add_empty_col_to_set(data_holder)
        for (name,), df in data_holder.data.group_by(self.source_col):
            name = str(name)
            _id = translate_dyntaxa.get_dyntaxa_id(name)
            if not _id:
                continue
            self._log(
                f"Adding {_id} to {self.col_to_set} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            self._add_to_col_to_set(data_holder, name, _id)
            # data_holder.data = data_holder.data.with_columns(
            #     pl.when(pl.col(self.source_col) == name)
            #     .then(pl.lit(_id))
            #     .otherwise(pl.col(self.col_to_set))
            #     .alias(self.col_to_set)
            # )


class PolarsAddTaxonRanks(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll", "bacterioplankton")
    ranks = (
        "kingdom",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "taxon_hierarchy",
    )
    cols_to_set = tuple(
        f"taxon_{rank}" if "taxon" not in rank else rank for rank in ranks
    )
    source_col = "dyntaxa_scientific_name"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds taxon rank columns. Data from dyntaxa."

    def _add_columns(self, data_holder: PolarsDataHolder):
        for col in self.cols_to_set:
            self._add_empty_col(data_holder, col)

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_dyntaxa:
            self._log(
                "Could not add taxon rank. Package nodc-dyntaxa not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        self._add_columns(data_holder=data_holder)
        for (name,), df in data_holder.data.group_by(self.source_col):
            info = dyntaxa_taxon.get_info(scientificName=name, taxonomicStatus="accepted")
            if not info:
                self._log(
                    f"Could not add information about taxon rank for {name} "
                    f"({len(df)} places)",
                    item=name,
                    level=adm_logger.WARNING,
                )
                continue
            if len(info) != 1:
                self._log(
                    f"Several matches in dyntaxa for {name} ({len(df)} places)",
                    item=name,
                    level=adm_logger.WARNING,
                )
                continue
            single_info = info[0]
            self._log(
                f"Adding taxon rank for {name} ({len(df)} places)",
                level=adm_logger.INFO,
            )
            for rank, col in zip(self.ranks, self.cols_to_set):
                value = single_info.get(rank, "") or ""
                data_holder.data = data_holder.data.with_columns(
                    pl.when(pl.col(self.source_col) == name)
                    .then(pl.lit(value))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
                self._log(
                    f"Adding {col} for {name} ({len(df)} places)",
                    level=adm_logger.DEBUG,
                )


class PolarsAddDyntaxaId(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "dyntaxa_scientific_name"
    col_to_set = "dyntaxa_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddDyntaxaId.col_to_set} translated from nodc_dyntaxa. "
            f"Source column is {AddDyntaxaId.source_col}"
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_dyntaxa:
            self._log(
                "Could not add dyntaxa id. Package nodc-dyntaxa not found/installed!",
                level=adm_logger.ERROR,
            )
            return
        if self.source_col not in data_holder.data.columns:
            self._log(
                f"Could not add column {self.col_to_set}. "
                f"Source column {self.source_col} not in data.",
                level=adm_logger.ERROR,
            )
            return
        if self.col_to_set not in data_holder.data.columns:
            self._log(f"Adding empty column {self.col_to_set}", level=adm_logger.DEBUG)
            self._add_empty_col_to_set(data_holder)

        for (name,), df in data_holder.data.group_by(self.source_col):
            if not str(name).strip():
                self._log(
                    f"Missing {self.source_col} when trying to add dyntaxa_id, "
                    f"{len(df)} rows.",
                    level=adm_logger.WARNING,
                )
                continue
            dyntaxa_id = dyntaxa_taxon.get(str(name))
            if not dyntaxa_id:
                self._log(
                    f"No {self.col_to_set} found for {name}, {len(df)} rows.",
                    level=adm_logger.WARNING,
                )
                continue
            self._log(
                f"Adding {self.col_to_set} {dyntaxa_id} translated from {name} "
                f"({len(df)} places)",
                level=adm_logger.INFO,
            )
            self._add_to_col_to_set(data_holder, name, dyntaxa_id)

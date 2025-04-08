from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, Transformer

try:
    import nodc_worms

    taxa_worms = nodc_worms.get_taxa_worms_object()
    translate_worms = nodc_worms.get_translate_worms_object()
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class AddReportedAphiaId(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "aphia_id"
    col_to_set = "reported_aphia_id"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddReportedAphiaId.col_to_set} "
            f"from {AddReportedAphiaId.source_col} if not given."
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set in data_holder.data.columns:
            adm_logger.log_transformation(
                f"Column {self.col_to_set} already in data. Will not add",
                level=adm_logger.DEBUG,
            )
            return
        if self.source_col not in data_holder.data.columns:
            adm_logger.log_transformation(
                f"No source column {self.source_col}. "
                f"Setting empty column {self.col_to_set}",
                level=adm_logger.DEBUG,
            )
            data_holder.data[self.col_to_set] = ""
            return
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]


class AddWormsScientificName(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "dyntaxa_scientific_name"
    col_to_set = "worms_scientific_name"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Adds {AddWormsScientificName.col_to_set} translated from nodc_worms. "
            f"Source column is {AddWormsScientificName.source_col}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = ""
        for name, df in data_holder.data.groupby(self.source_col):
            new_name = translate_worms.get(str(name))
            if new_name:
                adm_logger.log_transformation(
                    f"Translate worms: {name} -> {new_name} ({len(df)} places)"
                )
            else:
                new_name = name
            boolean = data_holder.data[self.source_col] == name
            data_holder.data.loc[boolean, self.col_to_set] = new_name


class AddWormsAphiaId(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "worms_scientific_name"
    col_to_set = "aphia_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def get_transformer_description() -> str:
        return f"Adds {AddWormsAphiaId.col_to_set} from {AddWormsAphiaId.source_col}"

    @adm_logger.log_time
    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.col_to_set not in data_holder.data.columns:
            adm_logger.log_transformation(
                f"Adding column {self.col_to_set}", level=adm_logger.DEBUG
            )
            data_holder.data[self.col_to_set] = ""

        for source_name, df in data_holder.data.groupby(self.source_col):
            try:
                aphia_id = taxa_worms.get_aphia_id(str(source_name))
            except Exception as e:
                adm_logger.log_transformation(
                    f"Could not find aphia_id for species {source_name}: {e}",
                    item=source_name,
                    level=adm_logger.WARNING,
                )
                continue
            if not aphia_id:
                adm_logger.log_transformation(
                    f"No aphia_id found for species {source_name}",
                    item=source_name,
                    level=adm_logger.WARNING,
                )
                continue
            boolean = data_holder.data[self.source_col] == source_name
            data_holder.data.loc[boolean, self.col_to_set] = aphia_id


class SetAphiaIdFromReportedAphiaId(Transformer):
    valid_data_types = ("plankton_imaging",)
    source_col = "reported_aphia_id"
    col_to_set = "aphia_id"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {SetAphiaIdFromReportedAphiaId.col_to_set} "
            f"from {SetAphiaIdFromReportedAphiaId.source_col} if it is a digit."
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]
        adm_logger.log_transformation(
            f"Setting {self.col_to_set} from {self.source_col}", level=adm_logger.DEBUG
        )

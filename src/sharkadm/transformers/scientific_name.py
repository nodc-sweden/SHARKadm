import polars as pl
from sharkadm.transformers.base import DataHolderProtocol, Transformer, PolarsTransformer
from sharkadm.data import PolarsDataHolder


class SetScientificNameFromReportedScientificName(Transformer):
    valid_data_types = ("plankton_imaging",)
    source_col = "reported_scientific_name"
    col_to_set = "scientific_name"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {SetScientificNameFromReportedScientificName.col_to_set} "
            f"from {SetScientificNameFromReportedScientificName.source_col} "
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]


class SetScientificNameFromDyntaxaScientificName(Transformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "dyntaxa_scientific_name"
    col_to_set = "scientific_name"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {SetScientificNameFromDyntaxaScientificName.col_to_set} "
            f"from {SetScientificNameFromDyntaxaScientificName.source_col} "
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = data_holder.data[self.source_col]


class PolarsSetScientificNameFromReportedScientificName(PolarsTransformer):
    valid_data_types = ("plankton_imaging",)
    source_col = "reported_scientific_name"
    col_to_set = "scientific_name"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsSetScientificNameFromReportedScientificName.col_to_set} "
            f"from {PolarsSetScientificNameFromReportedScientificName.source_col} "
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )


class PolarsSetScientificNameFromDyntaxaScientificName(PolarsTransformer):
    invalid_data_types = ("physicalchemical", "chlorophyll")
    source_col = "dyntaxa_scientific_name"
    col_to_set = "scientific_name"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsSetScientificNameFromDyntaxaScientificName.col_to_set} "
            f"from {PolarsSetScientificNameFromDyntaxaScientificName.source_col} "
        )

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.col(self.source_col).alias(self.col_to_set)
        )

import polars as pl

from sharkadm.data.archive import get_archive_data_holder_names

from .base import DataHolderProtocol, Transformer


class AddStaticDataHoldingCenterEnglish(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = "data_holding_centre"
    text_to_set = "Swedish Meteorological and Hydrological Institute (SMHI)"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {AddStaticDataHoldingCenterEnglish.col_to_set} "
            f"to {AddStaticDataHoldingCenterEnglish.text_to_set}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = self.text_to_set


class AddStaticDataHoldingCenterSwedish(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = "data_holding_centre"
    text_to_set = "Sveriges Meteorologiska och Hydrologiska Institut (SMHI)"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {AddStaticDataHoldingCenterSwedish.col_to_set} "
            f"to {AddStaticDataHoldingCenterSwedish.text_to_set}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data[self.col_to_set] = self.text_to_set


class PolarsAddStaticDataHoldingCenterEnglish(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = "data_holding_centre"
    text_to_set = "Swedish Meteorological and Hydrological Institute (SMHI)"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsAddStaticDataHoldingCenterEnglish.col_to_set} to "
            f"{PolarsAddStaticDataHoldingCenterEnglish.text_to_set}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit(self.text_to_set).alias(self.col_to_set)
        )


class PolarsAddStaticDataHoldingCenterSwedish(Transformer):
    valid_data_holders = get_archive_data_holder_names()
    col_to_set = "data_holding_centre"
    text_to_set = "Sveriges Meteorologiska och Hydrologiska Institut (SMHI)"

    @staticmethod
    def get_transformer_description() -> str:
        return (
            f"Sets {PolarsAddStaticDataHoldingCenterEnglish.col_to_set} to "
            f"{PolarsAddStaticDataHoldingCenterEnglish.text_to_set}"
        )

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.with_columns(
            pl.lit(self.text_to_set).alias(self.col_to_set)
        )

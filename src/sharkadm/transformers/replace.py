from sharkadm.sharkadm_logger import adm_logger

from .base import DataHolderProtocol, PolarsTransformer, Transformer


class ReplaceNanWithEmptyString(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Replaces all nan values in data with empty string"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data.fillna("", inplace=True)


class PolarsReplaceNanWithNone(PolarsTransformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Replaces all nan values in data with None"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if not data_holder.data.is_empty():
            data_holder.data = data_holder.data.fill_nan(None)
        else:
            adm_logger.log_transformation(
                "No data in data holder", level=adm_logger.WARNING
            )

from .base import DataHolderProtocol, Transformer


class ReplaceNanWithEmptyString(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Replaces all nan values in data with empty string"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data.fillna("", inplace=True)

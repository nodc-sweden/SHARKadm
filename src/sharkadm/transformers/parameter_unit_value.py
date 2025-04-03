from .base import Transformer, DataHolderProtocol


class RemoveRowsWithNoParameterValue(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return f"Removes rows where parameter value has no value"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data[~(data_holder.data["value"] == "")]

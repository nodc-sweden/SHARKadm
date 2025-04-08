from .base import DataHolderProtocol, Transformer


class RemoveRowsWithNoParameterValue(Transformer):
    @staticmethod
    def get_transformer_description() -> str:
        return "Removes rows where parameter value has no value"

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data[~(data_holder.data["value"] == "")]

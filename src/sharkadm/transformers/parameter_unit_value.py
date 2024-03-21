from .base import Transformer, DataHolderProtocol


class AddParameterUnitValueFromReported(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Sets parameter, unit and value from reported parameter, unit and value'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['parameter'] = data_holder.data['reported_parameter']
        data_holder.data['unit'] = data_holder.data['reported_unit']
        data_holder.data['value'] = data_holder.data['reported_value']


class RemoveRowsWithNoParameterValue(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Removes rows where parameter value has no value'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data[~(data_holder.data['value'] == '')]


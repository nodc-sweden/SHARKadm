from .base import Transformer, DataHolderProtocol


class FakeAddPressure(Transformer):
    valid_data_holders = ['LimsDataHolder']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds pressure = depth to lims data'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['PRES'] = data_holder.data['DEPH']
        data_holder.data['Q_PRES'] = data_holder.data['Q_DEPH']


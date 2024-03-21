from .base import Transformer, DataHolderProtocol


class AddVisitKey(Transformer):
    valid_data_types = ['physicalchemical']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds visit key column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data['visit_key'] = data_holder.data['datetime'].apply(lambda x: x.strftime('%Y%m%d_%H%M')).str.cat(
            data_holder.data['platform_code'], '_'
            ).str.cat(data_holder.data['visit_id'], '_')


from .base import Transformer, DataHolderProtocol


class AddCruiseId(Transformer):

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds cruise id column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.assign(
            visit_key=(
                    data_holder.data['visit_year'] +
                    '_' +
                    data_holder.data['platform_code'] +
                    '_' +
                    data_holder.data['cruise_no']
            )
        )
        # data_holder.data['cruise_id'] = data_holder.data['visit_year'] + '_' + data_holder.data['platform_code'] + '_' + data_holder.data['cruise_no']


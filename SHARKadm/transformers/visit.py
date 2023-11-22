import pandas as pd

from .base import Transformer, DataHolderProtocol


class AddVisitKey(Transformer):
    valid_data_types = ['physicalchemical']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds visit key column'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        data_holder.data = data_holder.data.assign(
            visit_key=(
                    data_holder.data['datetime'].apply(lambda x: x.strftime('%Y%m%d_%H%M')) +
                    '_' +
                    data_holder.data['platform_code'] +
                    '_' +
                    data_holder.data['visit_id']
            )
        )

        # data_holder.data['visit_key'] = pd.concat([
        #     data_holder.data['datetime'].apply(lambda x: x.strftime('%Y%m%d_%H%M')),
        #     data_holder.data['platform_code'],
        #     data_holder.data['visit_id']
        # ], axis=1, ignore_index=True).astype(str).agg('_'.join, axis=1)


        # data_holder.data['visit_key'] = data_holder.data['datetime'].apply(lambda x: x.strftime('%Y%m%d_%H%M')) + '_' \
        #                                 + '_' + data_holder.data['platform_code'] + '_' + data_holder.data['visit_id']


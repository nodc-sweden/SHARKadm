from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger
import numpy as np


class ManualSealPathology(Transformer):
    valid_data_types = ['SealPathology']
    valid_data_holders = ['ZipArchiveDataHolder']

    @staticmethod
    def get_transformer_description() -> str:
        return f'Manual fixes for SealPathology'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        md5 = '364768f88de5f22c0e415150eddee722'
        boolean = data_holder.data['shark_sample_id_md5'] == md5
        df = data_holder.data[boolean]
        if df.empty:
            adm_logger.log_transformation(f'md5 not found: {md5}', level=adm_logger.INFO)
            return
        data_holder.data.loc[boolean, 'visit_year'] = '2018'
        data_holder.data.loc[boolean, 'visit_month'] = '01'
        data_holder.data.loc[boolean, 'sample_date'] = '2018-01-01'


class ManualHarbourPorpoise(Transformer):
    valid_data_types = ['HarbourPorpoise']
    valid_data_holders = ['ZipArchiveDataHolder']
    source_col = 'observation_date'
    col_to_set = 'visit_date'

    @staticmethod
    def get_transformer_description() -> str:
        return f'Manual fixes for HarbourPorpoise'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if self.source_col not in data_holder.data:
            adm_logger.log_transformation(f'Source column {self.source_col} not found', level=adm_logger.DEBUG)
            return
        md5s = [
            'd15bc86c3b84b65c227da85d48db5091',
            '6f080a561c6e18b4ec3f436ea84cc33d',
            '549c1e46578ff95d694cfb21139ffc67',
            '777f6330cccafaa4de98bbebba2fa76b',
            '634b1bc7ae5b70cf65bb1699acfd8b2e',
            'f96e51d95f867a316ed09b2724e2e9d2',
            '6fd78ab27999e2b08a2d6cdb494dc14c',
            '664ee45cb5fe541867ea00edf9b83ba6',
        ]
        for md5 in md5s:
            boolean = data_holder.data['shark_sample_id_md5'] == md5
            df = data_holder.data[boolean]
            if df.empty:
                adm_logger.log_transformation(f'md5 not found: {md5}', level=adm_logger.DEBUG)
                continue
            data_holder.data.loc[boolean, self.col_to_set] = data_holder.data.loc[boolean, self.source_col]
            adm_logger.log_transformation(f'{self.col_to_set} set from {self.source_col} for md5={md5} in {len(np.where(boolean)[0])} places',
                                          level=adm_logger.INFO)

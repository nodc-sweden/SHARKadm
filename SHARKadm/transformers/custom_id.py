from SHARKadm import config
from SHARKadm.config import column_info
from SHARKadm.config import data_type_mapper
from SHARKadm.config import custom_id
from .base import Transformer, DataHolderProtocol

import hashlib
from SHARKadm import adm_logger


class AddCustomId(Transformer):
    _id_handler = config.get_custom_id_handler()

    def __init__(self, add_md5: bool = False):
        super().__init__()
        self._add_md5 = add_md5

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds custom key and md5 id'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        """custom_id in taken from self._id_handler"""
        for level in self._id_handler.get_levels_for_datatype(data_holder.data_type):
            id_handler = self._id_handler.get_level_handler(data_type=data_holder.data_type,
                                                            level=level,
                                                            )
            col_name = f'{level}_id'
            missing = set(id_handler.id_columns) - set(data_holder.data.columns)
            if missing:
                adm_logger.log_transformation(f'Missing columns for creating {col_name}: {", ".join(list(missing))}',
                                              level='warning')
                continue
            data_holder.data[col_name] = data_holder.data.apply(lambda row: id_handler.get_id(row), axis=1)
            if self._add_md5:
                col_name_md5 = f'{col_name}_md5'
                data_holder.data[col_name_md5] = data_holder.data[col_name].apply(self.get_md5)
        # if 'sharkadm_sample_id_md5' in data_holder.data.columns:
        #     data_holder.data['shark_sample_id_md5'] = data_holder.data['sharkadm_sample_id_md5']

    @staticmethod
    def get_md5(x) -> str:
        return hashlib.md5(x.encode('utf-8')).hexdigest()



from SHARKadm import config
from SHARKadm.config import column_info
from SHARKadm.config import data_type_mapper
from SHARKadm.config import sharkadm_id
from .base import Transformer, DataHolderProtocol

import hashlib


class AddSharkadmIdToColumns(Transformer):

    def __init__(self,
                 column_info: column_info.ColumnInfoConfig = None,
                 id_handler: sharkadm_id.SharkadmIdHandler = None,
                 d_type_mapper: data_type_mapper.DataTypeMapper = None,
                 ) -> None:
        self._column_info = column_info
        self._id_handler = id_handler
        self._d_type_mapper = d_type_mapper

    @classmethod
    def from_default_config(cls):
        col_info = config.get_column_info_config()
        id_handler = config.get_sharkadm_id_handler()
        d_type_mapper = config.get_data_type_mapper()
        return AddSharkadmIdToColumns(
            column_info=col_info,
            id_handler=id_handler,
            d_type_mapper=d_type_mapper
        )

    def transform(self, data_holder: DataHolderProtocol) -> None:
        """sharkadm_id in taken from self._id_handler"""
        for level in self._column_info.all_levels:
            id_handler = self._id_handler.get_level_handler(data_type=data_holder.data_type,
                                                            level=level,
                                                            data_type_mapper=self._d_type_mapper)
            if not id_handler:
                # new_data[f'sharkadm_{level}_id'] = ''
                continue
            col_name = f'sharkadm_{level}_id'
            col_name_md5 = f'{col_name}_md5'
            data_holder.data[col_name] = data_holder.data.apply(lambda row: id_handler.get_id(row), axis=1)
            data_holder.data[col_name_md5] = data_holder.data[col_name].apply(self.get_md5)

    @staticmethod
    def get_md5(x) -> str:
        return hashlib.md5(x.encode('utf-8')).hexdigest()

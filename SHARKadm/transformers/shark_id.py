from SHARKadm import config
from .base import Transformer, DataHolderProtocol

import hashlib
from SHARKadm import adm_logger


class AddSharkId(Transformer):

    def __init__(self, add_md5: bool = True):
        super().__init__()
        self._add_md5 = add_md5
        self._cached_md5 = {}

    @staticmethod
    def get_transformer_description() -> str:
        return 'Adds shark_id and shark_md5_id'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        import_matrix = config.get_import_matrix_config(data_type=data_holder.data_type)
        for level, cols in import_matrix.get_columns_by_level().items():
            col_name = f'shark_{level}_id'
            # missing = set(cols) - set(data_holder.data.columns)
            # if missing:
            #     adm_logger.log_transformation(f'Missing columns for creating {col_name}: {", ".join(list(missing))}',
            #                                   level='warning')
            #     continue
            data_holder.data[col_name] = data_holder.data.apply(lambda row,
                                                                       dtype=data_holder.data_type,
                                                                       cols=cols: self._get_id(row, dtype, cols),
                                                                axis=1)
            if self._add_md5:
                col_name_md5 = f'{col_name}_md5'
                data_holder.data[col_name_md5] = data_holder.data[col_name].apply(self.get_md5)

    def _get_id(self, row: dict, dtype: str, cols: list[str]) -> str:
        """Returns the id based on the given data"""
        parts = [dtype]
        for col in cols:
            if not row.get(col):
                continue
            parts.append(row[col].replace('/', '_'))
        return '_'.join(parts)

    def get_md5(self, x) -> str:
        return self._cached_md5.setdefault(x, hashlib.md5(x.encode('utf-8')).hexdigest())



import pandas as pd

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

            if not all([col in data_holder.data.columns for col in cols]):
                adm_logger.log_transformation(f'Can not create shark_id. All columns are not in data',
                                              add=', '.join(cols), level=adm_logger.WARNING)
                continue

            data_holder.data[col_name] = data_holder.data[cols].apply('_'.join, axis=1).replace('/', '_')
            # data_holder.data[col_name] = data_holder.data.apply(lambda row,
            #                                                            dtype=data_holder.data_type,
            #                                                            cols=cols: self._get_id(row, cols),
            #                                                     axis=1)
            if self._add_md5:
                col_name_md5 = f'{col_name}_md5'
                data_holder.data[col_name_md5] = data_holder.data[col_name].apply(self.get_md5)

    def _get_id(self, row: pd.Series, cols: list[str]) -> str:
        """Returns the id based on the given data"""
        # print()
        # print(f'{row=}')
        # print(f'{dtype=}')
        # print(f'{cols=}')
        # parts = [dtype]
        for col in cols:
            try:
                if row.get(col):
                    pass
            except ValueError:
                print()
                print(col)
                print('-'*100)
                print(row.get(col))
        parts = [row[col].replace('/', '_') for col in cols if row.get(col)]
        # for col in cols:
        #     # print('='*100)
        #     # print('='*100)
        #     # print(f'=== {type(row)=}')
        #     # cols = [col for col in row.keys() if 'water' in col]
        #     # print(f'=== {cols=}')
        #     # print(f'=== {col=}')
        #     # print(f'- {row.get(col)=}')
        #     if not row.get(col):
        #         continue
        #     parts.append(row[col].replace('/', '_'))
        return '_'.join(parts)

    def get_md5(self, x) -> str:
        return self._cached_md5.setdefault(x, hashlib.md5(x.encode('utf-8')).hexdigest())



from .base import Transformer, DataHolderProtocol
from sharkadm import adm_logger


class ManualSealPathology(Transformer):
    valid_data_types = ['SealPathology']
    valid_data_holders = ['ZipArchiveDataHolder']

    def __init__(self, apply_on_columns: list[str] | None = None, **kwargs):
        super().__init__(**kwargs)
        if apply_on_columns:
            self.apply_on_columns = apply_on_columns

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

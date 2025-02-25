from sharkadm.config import get_column_views_config
from .base import FileExporter, DataHolderProtocol


class ExportColumnViewsColumnsNotInData(FileExporter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._column_views = get_column_views_config()

    @staticmethod
    def get_exporter_description() -> str:
        return f'Writes all columns in column_views that are not in data'

    def _export(self, data_holder: DataHolderProtocol) -> None:
        if not self._export_file_name:
            self._export_file_name = f'missing_column_views_{data_holder.dataset_name}.txt'

        cols_to_write = []
        column_views_columns = self._column_views.get_columns_for_view(data_holder.data_type_internal)
        for col in column_views_columns:
            if col in data_holder.data:
                continue
            cols_to_write.append(col)
        with open(self.export_file_path, 'w') as fid:
            fid.write('\n'.join(cols_to_write))
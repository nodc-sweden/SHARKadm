from sharkadm.data.archive import PolarsArchiveDataHolder
from sharkadm.data.dv_template import DPSTemplateDataHolder


class DPSArchiveDataHolder(DPSTemplateDataHolder, PolarsArchiveDataHolder):
    def __init__(self, archive_root_directory, *, dtype: str, **kwargs):
        self._data_type_synonym = dtype
        super().__init__(archive_root_directory=archive_root_directory, **kwargs)

        self._data_format = self.delivery_note.import_matrix_key
        self._dps_version = self._data_format.split("_")[1]

        self._set_template_path(self.delivery_note.data_type.lower(), self._dps_version)
        self._load_validation_columns()

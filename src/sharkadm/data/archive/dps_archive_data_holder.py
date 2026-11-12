from sharkadm.data.archive import PolarsArchiveDataHolder
from sharkadm.data.dv_template import DPSTemplateDataHolder


class DPSArchiveDataHolder(DPSTemplateDataHolder, PolarsArchiveDataHolder):
    # TODO make general by getting data_type from delivery_note
    _data_type_synonym = "phytoplankton"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._data_format = self.delivery_note.import_matrix_key
        self._dps_version = self._data_format.split("_")[1]

        self._set_template_path(self.delivery_note.data_type.lower(), self._dps_version)
        self._load_validation_columns()

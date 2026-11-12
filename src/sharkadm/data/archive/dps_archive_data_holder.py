from sharkadm.data.archive import PolarsArchiveDataHolder
from sharkadm.data.dv_template import DPSTemplateDataHolder


class DPSArchiveDataHolder(DPSTemplateDataHolder, PolarsArchiveDataHolder):
    # TODO make general by getting data_type from delivery_note
    _data_type_synonym = "phytoplankton"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_dps_template_state()
        self._set_template_path_from_metadata(
            self.delivery_note.data_type, self.delivery_note.data_format
        )
        self._load_validation_columns()

from sharkadm import event
from sharkadm.sharkadm_logger import adm_logger

from ..data import PolarsDataHolder
from ..utils import svn
from .base import PolarsTransformer

nodc_occurrence_id = None
try:
    import nodc_occurrence_id
    from nodc_occurrence_id import event as occurrence_event
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(
        f'Could not import package "{module_name}" in module {__name__}. '
        f"You need to install this dependency if you want to use this module.",
        level=adm_logger.WARNING,
    )


class AddOccurrenceId(PolarsTransformer):
    valid_data_types = ("zoobenthos", "plankton_imaging")

    def __init__(self, *args, add_if_valid: bool = True, **kwargs):
        super().__init__(*args, *kwargs)
        self._add_if_valid = add_if_valid
        self.col_to_set = ""  # Is set in self._transform
        self.database = None
        self.valid_matches = []
        self._svn_commit = kwargs.get("svn_commit", False)

    @staticmethod
    def get_transformer_description() -> str:
        return "Adds Occurrence Id to data"

    def _transform(self, data_holder: PolarsDataHolder) -> None:
        if not nodc_occurrence_id:
            self._log(
                "Missing package nodc_occurrence_id to add occurrence_id",
                level=adm_logger.ERROR,
            )
            return
        self._current_data_holder = data_holder

        # 1: Kolla om perfekt match finns i databas
        #    Lägg till dessa id i data

        # 2: Om EJ nära perfekt match i databas.
        #    Skapa nytt id och lägg till i data och databas

        # 3: Om när match. Logga _temp_occurence_id som man sedan kan sätta nya ????

        occurrence_event.subscribe(
            "missing_mandatory_columns", self._on_missing_mandatory_columns
        )

        occurrence_event.subscribe("progress", self._on_progress)
        occurrence_event.subscribe("result", self._on_result)

        self.database = nodc_occurrence_id.get_occurrence_database_for_data_type(
            data_holder.data_type_internal
        )
        self.col_to_set = self.database.id_column
        self.valid_matches = []
        self.database.add_uuid_to_data_and_database(
            data_holder.data, add_if_valid=self._add_if_valid
        )

        if self._svn_commit:
            svn.commit_files(self.database.db_path)

    def _on_missing_mandatory_columns(self, data: dict) -> None:
        self._svn_commit = False
        self._log(
            f"Could not add {self.col_to_set}. "
            f"Missing columns: {data['missing_columns']}",
            level=adm_logger.WARNING,
        )

    def _on_progress(self, data: dict) -> None:
        event.post_event("progress", data)

    def _on_result(self, data: dict) -> None:
        self._log(data.get("msg", ""), level=adm_logger.INFO)

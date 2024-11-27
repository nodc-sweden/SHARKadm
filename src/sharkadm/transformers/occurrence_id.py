from sharkadm import adm_logger
from .base import Transformer, DataHolderProtocol

nodc_occurrence_id = None
try:
    import nodc_occurrence_id
    from nodc_occurrence_id import event as occurrence_event
except ModuleNotFoundError as e:
    module_name = str(e).split("'")[-2]
    adm_logger.log_workflow(f'Could not import package "{module_name}" in module {__name__}. You need to install this dependency if you want to use this module.', level=adm_logger.WARNING)


class AddOccurrenceId(Transformer):
    valid_data_types = ['zoobenthos', 'ifcb']

    col_to_set = ''  # Is set in self._transform
    database = None
    valid_matches = []

    @staticmethod
    def get_transformer_description() -> str:
        return f'Adds Occurrence Id to data'

    def _transform(self, data_holder: DataHolderProtocol) -> None:
        if not nodc_occurrence_id:
            adm_logger.log_transformation(
                f'Missing package nodc_occurrence_id to add occurrence_id',
                level=adm_logger.ERROR)
            return
        self._current_data_holder = data_holder

        # 1: Kolla om perfekt match finns i databas
        #    Lägg till dessa id i data

        # 2: Om EJ nära perfekt match i databas. Skapa nytt id och lägg till i data och databas

        # 3: Om när match. Logga _temp_occurence_id som man sedan kan sätta nya

        occurrence_event.subscribe('missing_mandatory_columns', self._on_missing_mandatory_columns)

        occurrence_event.subscribe('id_added_to_data_from_database', self._on_id_added_to_data_from_database)
        occurrence_event.subscribe('new_id_added_to_data_and_database', self._on_new_id_added_to_data_and_database)
        occurrence_event.subscribe('several_valid_matches_in_database', self._on_several_valid_matches_in_database)
        occurrence_event.subscribe('valid_match_in_database', self._on_valid_match_in_database)

        print(f'{data_holder.data_type=}')
        self.database = nodc_occurrence_id.get_occurrence_database_for_data_type(data_holder.data_type)
        print(f'{self.database.db_path=}')
        self.col_to_set = self.database.id_column
        self.valid_matches = []
        self.database.add_uuid_to_data_and_database(data_holder.data)

    def add_valid_matches(self):
        self.database.add_matching_to_data(*self.valid_matches)

    def _on_missing_mandatory_columns(self, data: dict) -> None:
        adm_logger.log_transformation(f'Could not add {self.col_to_set}. Missing columns: {data["missing_columns"]}',
                                      level=adm_logger.WARNING)

    def _on_id_added_to_data_from_database(self, data: dict) -> None:
        perfect = 'no perfect match'
        if data['perfect_match_in_db']:
            perfect = 'perfect match'
        if data.get('forced'):
            adm_logger.log_transformation(
                f'Existing ({perfect}) {self.col_to_set} ({data["id"]}) added "by force" ({data["nr_places"]})',
                level=adm_logger.INFO)
        else:
            adm_logger.log_transformation(
                f'Existing ({perfect}) {self.col_to_set} ({data["id"]}) added ({data["nr_places"]})',
                level=adm_logger.INFO)

    def _on_new_id_added_to_data_and_database(self, data: dict) -> None:
        adm_logger.log_transformation(f'New {self.col_to_set} ({data["id"]}) added ({data["nr_places"]})',
                                      level=adm_logger.INFO)
        adm_logger.log_transformation(f'New {self.col_to_set} ({data["id"]}) added to database',
                                      level=adm_logger.DEBUG)

    def _on_several_valid_matches_in_database(self, data: dict) -> None:
        adm_logger.log_transformation(f'Several valid matches in database for {self.col_to_set}: {data["temp_id_str"]}',
                                      level=adm_logger.WARNING)

    def _on_valid_match_in_database(self, data: dict) -> None:
        self.valid_matches.append(data['valid_match'])
        adm_logger.log_transformation(f'Valid match found in database for {self.col_to_set}: {data["temp_id_str"]}',
                                      level=adm_logger.WARNING)


from typing import Protocol


class DataHolderProtocol(Protocol):
    def get_original_name(self, item: str) -> str:
        ...


class Feedback:

    @staticmethod
    def missing_in_analyse_info(par: str, data_holder: DataHolderProtocol, **kwargs):
        original_parameter_name = data_holder.get_original_name(par)
        return f'Information om parameter "{original_parameter_name}" saknas i Analysinfo'

    @staticmethod
    def invalid_date_in_analys_info(dtime: str):
        return f'Ogiltigt datum eller datumformat i Analysinfo: {dtime}'

    @staticmethod
    def missing_position(rows: list):
        if len(rows) >= 20:
            return 'Position saknas på fler än 20 rader'
        else:
            row_str = ', '.join(sorted(rows, key=int))
            return f'Position saknas på följande rader: {row_str}'

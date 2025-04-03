from typing import Protocol
import datetime

from sharkadm.sharkadm_logger.base import SharkadmLoggerExporter
from sharkadm.utils.paths import get_next_incremented_file_path


class DataHolderProtocol(Protocol):
    def get_original_name(self, item: str) -> str: ...


class Feedback:
    @staticmethod
    def missing_in_analyse_info(par: str, data_holder: DataHolderProtocol, **kwargs):
        original_parameter_name = data_holder.get_original_name(par)
        return f'Information om parameter "{original_parameter_name}" saknas i Analysinfo'

    @staticmethod
    def invalid_date_in_analys_info(dtime: str):
        return f"Ogiltigt datum eller datumformat i Analysinfo: {dtime}"

    @staticmethod
    def missing_position(rows: list):
        if len(rows) >= 20:
            return "Position saknas på fler än 20 rader"
        else:
            row_str = ", ".join(sorted(rows, key=int))
            return f"Position saknas på följande rader: {row_str}"


class FeedbackTxtExporter(SharkadmLoggerExporter):
    level_mapper = dict(
        error="Måste åtgärdas", warning="Bör åtgärdas", info="Se gärna över"
    )

    def _get_default_file_name(self):
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        file_name = f"feedback_{self.adm_logger.name}_{date_str}"
        return file_name

    def _export(self) -> None:
        self._set_save_path(suffix=".txt")
        lines = self._extract_info(self.adm_logger.data)
        try:
            self._save_txt(lines)
        except PermissionError:
            self.file_path = get_next_incremented_file_path(self.file_path)
            self._save_txt(lines)

    def _extract_info(self, data: dict) -> list[str]:
        lines = []
        for level, level_data in data.items():
            for purpose, purpose_data in level_data.items():
                if purpose != "feedback":
                    continue
                for log_type, log_type_data in purpose_data.items():
                    for msg, msg_data in log_type_data.items():
                        action = self.level_mapper.get(level)
                        if action:
                            msg = msg + f" ({action})"
                        lines.append(msg)
        return lines

    def _save_txt(self, lines: list[str]) -> None:
        with open(self.file_path, "w") as fid:
            fid.write("\n".join(lines))

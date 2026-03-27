from .base import SharkadmLoggerExporter


class PrintWarnings(SharkadmLoggerExporter):
    def _export(self) -> None:
        self.adm_logger.filter(">warning")
        for info in self.adm_logger.data:
            print(info["log_type"].ljust(30), info["cls"].ljust(30), info["msg"])
        self.adm_logger.reset_filter()

    def _get_default_file_name(self):
        return ""

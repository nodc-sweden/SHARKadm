from .sharkadm_logger import SHARKadmLogger
from .exporter import XlsxExporter


def create_xlsx_report(logger: SHARKadmLogger, **kwargs):
    exp = XlsxExporter(**kwargs)
    logger.export(exp)


adm_logger = SHARKadmLogger()

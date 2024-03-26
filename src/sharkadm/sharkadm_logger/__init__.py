from .sharkadm_logger import SHARKadmLogger
from .exporter import XlsxExporter


def create_xlsx_report(logger: SHARKadmLogger, filter: dict = None, **kwargs):
    if filter:
        logger.reset_filter()
        logger.filter(**filter)
    exp = XlsxExporter(**kwargs)
    logger.export(exp)


adm_logger = SHARKadmLogger()

from .sharkadm_logger import SHARKadmLogger
from .xlsx import XlsxExporter
from .txt import TxtExporter
from .feedback import FeedbackTxtExporter
from .print_on_screen import PrintWarnings


exporter_mapping = {
    'xlsx': XlsxExporter,
    'txt': TxtExporter,
    'feedback': FeedbackTxtExporter,
    'print_warnings': PrintWarnings,
}


def get_exporter(**kwargs):
    name = kwargs.pop('name')
    obj = exporter_mapping.get(name)
    if not obj:
        raise KeyError(f'Invalid sharkadm exporter name: {name}')
    return obj(**kwargs)


def create_xlsx_report(logger: SHARKadmLogger, filter: dict = None, **kwargs):
    if filter:
        logger.reset_filter()
        logger.filter(**filter)
    exp = XlsxExporter(**kwargs)
    logger.export(exp)


def create_txt_report(filter: dict = None, **kwargs):
    if filter:
        adm_logger.reset_filter()
        adm_logger.filter(**filter)
    exp = TxtExporter(**kwargs)
    adm_logger.export(exp)


def create_changelog_file(**kwargs):
    adm_logger.reset_filter()
    adm_logger.filter(levels='>info')
    exp = TxtExporter(file_name='changelog.txt', **kwargs)
    adm_logger.export(exp)


def create_feedback_report(logger: SHARKadmLogger, filter: dict = None, **kwargs):
    if filter:
        logger.reset_filter()
        logger.filter(**filter)
    exp = FeedbackTxtExporter(**kwargs)
    logger.export(exp)


def print_warnings(logger: SHARKadmLogger):
    exp = PrintWarnings()
    logger.export(exp)


adm_logger = SHARKadmLogger()

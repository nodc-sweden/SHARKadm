from .sharkadm_logger import SHARKadmLogger
from .xlsx import XlsxExporter
from .feedback import FeedbackTxtExporter


exporter_mapping = {
    'xlsx': XlsxExporter
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


def create_feedback_report(logger: SHARKadmLogger, filter: dict = None, **kwargs):
    if filter:
        logger.reset_filter()
        logger.filter(**filter)
    exp = FeedbackTxtExporter(**kwargs)
    logger.export(exp)


adm_logger = SHARKadmLogger()

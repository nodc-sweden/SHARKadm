import logging

from SHARKadm import event

logger = logging.getLogger(__name__)


class SHARKadmLogger:
    """Class to log events etc. in the SHARKadm dataflow"""
    DEBUG = 'debug'
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'

    def __init__(self):
        self._levels: list[str] = [
            'debug',
            'info',
            'warning',
            'error'
        ]

        self._transformations: dict = dict()
        self._validations: dict = dict()
        self._exports: dict = dict()
        self._workflow: dict = dict()

        self._initiate_log()

    def _initiate_log(self) -> None:
        """Initiate the log"""
        self._transformations: dict = dict((lev, {}) for lev in self._levels)
        self._validations: dict = dict((lev, {}) for lev in self._levels)
        self._workflow: dict = dict((lev, {}) for lev in self._levels)

    def _check_level(self, level: str) -> str:
        level = level.lower()
        if level not in self._levels:
            msg = f'Invalid level: {level}'
            logger.error(msg)
            raise KeyError(msg)
        return level

    def log_workflow(self, msg: str, level: str = 'info') -> None:
        level = self._check_level(level)
        self._workflow[level].setdefault(msg, 0)
        self._workflow[level][msg] += 1
        event.post_event('workflow', msg)

    def log_transformation(self, msg: str, level: str = 'info') -> None:
        level = self._check_level(level)
        self._transformations[level].setdefault(msg, 0)
        self._transformations[level][msg] += 1

    def log_validation(self, msg: str, level: str = 'info') -> None:
        level = self._check_level(level)
        self._validations[level].setdefault(msg, 0)
        self._validations[level][msg] += 1

    def log_exports(self, msg: str, level: str = 'info') -> None:
        level = self._check_level(level)
        self._exports[level].setdefault(msg, 0)
        self._exports[level][msg] += 1

    def reset_log(self) -> None:
        """Resets all entries to the log"""
        logger.info(f'Resetting {self.__class__.__name__}')
        self._initiate_log()

    def get_log_info(self) -> dict:
        info = dict()
        info['validations'] = self._validations
        info['transformations'] = self._transformations
        info['exports'] = self._exports
        info['workflow'] = self._workflow
        return info


adm_logger = SHARKadmLogger()

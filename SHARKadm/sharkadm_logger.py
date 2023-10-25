import logging

logger = logging.getLogger(__name__)


class SHARKadmLogger:
    """Class to log events etc. in the SHARKadm dataflow"""
    def __init__(self):
        self._levels: list[str] = [
            'info',
            'warning',
            'error'
        ]

        self._transformations: dict = dict()
        self._validations: dict = dict()
        self._workflow: list = list()

        self._initiate_log()

    def _initiate_log(self) -> None:
        """Initiate the log"""
        self._transformations: dict = dict((lev, {}) for lev in self._levels)
        self._validations: dict = dict((lev, {}) for lev in self._levels)
        self._workflow: list = list()

    def _check_level(self, level: str) -> str:
        level = level.lower()
        if level not in self._levels:
            msg = f'Invalid level: {level}'
            logger.error(msg)
            raise KeyError(msg)
        return level

    def log_workflow(self, msg: str) -> None:
        self._workflow.append(msg)

    def log_transformation(self, msg: str, level: str = 'info') -> None:
        level = self._check_level(level)
        self._transformations[level].setdefault(msg, 0)
        self._transformations[level][msg] += 1

    def log_validation(self, msg: str, level: str = 'info') -> None:
        level = self._check_level(level)
        self._validations[level].setdefault(msg, 0)
        self._validations[level][msg] += 1

    def reset_log(self) -> None:
        """Resets all entries to the log"""
        logger.info(f'Resetting {self.__class__.__name__}')
        self._initiate_log()


adm_logger = SHARKadmLogger()
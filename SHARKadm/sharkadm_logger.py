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

        self._transformations: dict = dict((lev, {}) for lev in self._levels)
        self._validations: dict = dict((lev, {}) for lev in self._levels)

    def _check_level(self, level: str) -> None:
        if level not in self._levels:
            msg = f'Invalid level: {level}'
            logger.error(msg)
            raise KeyError(msg)

    def log_transformation(self, msg: str, level: str = 'info') -> None:
        self._check_level(level)
        self._transformations[level].setdefault(msg, 0)
        self._transformations[level][msg] += 1

    def log_validation(self, msg: str, level: str = 'info') -> None:
        self._check_level(level)
        self._validations[level].setdefault(msg, 0)
        self._validations[level][msg] += 1


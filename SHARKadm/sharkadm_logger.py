import logging

logger = logging.getLogger(__name__)


class SHARKadmLogger:
    """Class to log events etc. in the SHARKadm dataflow"""
    def __init__(self):
        self._transformations: dict = {}
        self._validations: dict = {}

    def log_transformation(self, trans: str, message: str):
        print(f'{trans=}: {message=}')
        self._transformations.setdefault(trans, {})
        self._transformations[trans].setdefault(message, 0)
        self._transformations[trans][message] += 1


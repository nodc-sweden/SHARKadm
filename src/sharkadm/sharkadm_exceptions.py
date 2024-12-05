from sharkadm import adm_logger


class SHARKadmException(Exception):
    level = adm_logger.WARNING

    def __init__(self, msg, *args, **kwargs):
        level = kwargs.pop('level', self.level)
        extended_msg = f'(EXCEPTION ERROR) {self.__class__.__name__}: {msg}'
        adm_logger.log_workflow(extended_msg, level=level)
        super().__init__(msg, *args, **kwargs)


class DeliveryNoteError(SHARKadmException):
    pass


class NoDataFormatFoundError(SHARKadmException):
    pass


class ArchiveDataError(SHARKadmException):
    pass


class ArchiveDataHolderError(SHARKadmException):
    pass


class DataHolderError(SHARKadmException):
    pass


class InvalidTransformer(SHARKadmException):
    pass


class InvalidWorkflow(SHARKadmException):
    pass


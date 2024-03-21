from sharkadm import adm_logger


class SHARKadmException(Exception):
    def __init__(self, msg, *args, **kwargs):
        extended_msg = f'(EXCEPTION ERROR){self.__class__.__name__}: {msg}'
        adm_logger.log_workflow(extended_msg, level=adm_logger.WARNING)
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


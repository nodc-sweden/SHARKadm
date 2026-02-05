import enum

from sharkadm import adm_logger, config, event
from sharkadm.data import PolarsDataHolder, is_valid_polars_data_holder


class OperationType(enum.StrEnum):
    OPERATION = "operator"
    VALIDATOR = "validator"
    TRANSFORMER = "transformer"


class Operator:
    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    operation_type: str = OperationType.OPERATION

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def _data_holder_has_valid_data_type(self, data_holder: PolarsDataHolder) -> bool:
        if data_holder.data_type_internal == "unknown":
            return True
        if data_holder.data_type_internal in config.get_valid_data_types(
            valid=self.valid_data_types, invalid=self.invalid_data_types
        ):
            return True
        return False

    def _data_holder_is_valid_data_holder(self, data_holder: PolarsDataHolder) -> bool:
        if is_valid_polars_data_holder(
            data_holder,
            valid=self.valid_data_holders,
            invalid=self.invalid_data_holders,
        ):
            return True
        return False

    def _data_holder_has_valid_data_structure(
        self, data_holder: PolarsDataHolder
    ) -> bool:
        if data_holder.data_structure.lower() in config.get_valid_data_structures(
            valid=self.valid_data_structures, invalid=self.invalid_data_structures
        ):
            return True
        return False

    def is_valid_data_holder(self, data_holder: PolarsDataHolder) -> bool:
        if not self._data_holder_has_valid_data_type(data_holder):
            self._log_workflow(
                f"Invalid data_type {data_holder.data_type_internal} "
                f"for {self.operation_type}"
                f" {self.name}",
                level=adm_logger.DEBUG,
            )
            return False
        if not self._data_holder_is_valid_data_holder(data_holder):
            self._log_workflow(
                f"Invalid data_holder {data_holder.__class__.__name__} "
                f"for {self.operation_type}"
                f" {self.name}"
            )
            return False
        if not self._data_holder_has_valid_data_structure(data_holder):
            self._log_workflow(
                f"Invalid data structure {data_holder.data_structure} "
                f"for {self.operation_type} {self.name}",
                level=adm_logger.DEBUG,
            )
            return False
        return True

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)


class OperationInfo:
    """This class is used as a return for operators
    (validator, transformer, exporter) to give information about the outcome."""

    def __init__(
        self,
        operator: Operator = None,
        valid: bool = True,
        success: bool = True,
        exception: Exception | None = None,
        msg: str = "",
        cause_for_termination: bool = False,
    ):
        self._operator = operator
        self._valid = valid
        self._success = success
        self._exception = exception
        self._msg = msg
        self._cause_for_termination = cause_for_termination
        self._has_subscribed: bool = False
        self._logs = list()

        self._subscribe()

    def __repr__(self):
        return f"{self.__class__.__name__} for operator: {self.operator}"

    def _subscribe(self):
        if not self.operator:
            return
        if self._has_subscribed:
            return

    @property
    def operator(self) -> Operator:
        return self._operator

    @operator.setter
    def operator(self, operator: Operator):
        if self._operator:
            if self._operator == operator:
                return
            raise ValueError("Cannot set new operator!")
        if not isinstance(operator, Operator):
            raise ValueError("Invalid class!")
        self._operator = operator
        self._subscribe()

    @property
    def valid(self) -> bool:
        return self._valid

    @valid.setter
    def valid(self, is_valid: bool) -> None:
        self._valid = bool(is_valid)

    @property
    def success(self) -> bool:
        return self._success

    @property
    def exception(self) -> Exception:
        return self._exception

    @property
    def msg(self) -> str:
        return self._msg

    @property
    def logs(self) -> list[dict]:
        return self._logs

    @property
    def cause_for_termination(self) -> bool:
        return self._cause_for_termination

    def _on_log(self, data: dict):
        if not self.operator:
            return
        # print(f"::: {self.operator.name=} -> {data["cls"]=}")
        # print(f"{data=}")
        # print()
        if self.operator.name != data["cls"]:
            return
        self._logs.append(data)
        # print(f"_on_log {data['cls']=}: {data=}")
        # print()
        # print()

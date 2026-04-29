import enum
from dataclasses import dataclass
from typing import Any, Self, get_type_hints

from sharkadm import adm_logger, config
from sharkadm.data import PolarsDataHolder, is_valid_polars_data_holder


class OperatorType(enum.StrEnum):
    OPERATOR = "operator"
    VALIDATOR = "validator"
    TRANSFORMER = "transformer"


class Operator:
    valid_data_types: tuple[str, ...] = ()
    invalid_data_types: tuple[str, ...] = ()

    valid_data_holders: tuple[str, ...] = ()
    invalid_data_holders: tuple[str, ...] = ()

    valid_data_structures: tuple[str, ...] = ()
    invalid_data_structures: tuple[str, ...] = ()

    operation_type: str = OperatorType.OPERATOR

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
            print("-----")
            print(data_holder.data_structure.lower())
            print(
                config.get_valid_data_structures(
                    valid=self.valid_data_structures, invalid=self.invalid_data_structures
                )
            )
            print("TRUE")
            return True
        print("FALSE")
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
                level=adm_logger.ERROR,
            )
            return False
        return True

    def _log_workflow(self, msg: str, **kwargs):
        adm_logger.log_workflow(msg, cls=self.__class__.__name__, **kwargs)


@dataclass(kw_only=True)
class OperatorInfo:
    operator: Operator | None = None
    exception: Exception | None = None
    msg: str = ""
    cause_for_termination: bool = False
    valid: bool = True
    success: bool = True
    """This class is used as a return for operators
    (validator, transformer, exporter) to give information about the outcome."""

    def __setattr__(self, key: str, value: Any) -> None:
        hints = get_type_hints(type(self))

        if not hints.get(key):
            raise AttributeError(f"Unknown attribute {key}")
        expected = hints[key]
        if not isinstance(value, expected):
            raise TypeError(
                f"{key} must be {expected.__name__}, got {type(value).__name__}"
            )
        super().__setattr__(key, value)

    def __add__(self, other: Self):
        infos = OperatorsInfo()
        infos.add(self)
        if isinstance(other, OperatorInfo):
            infos.add(other)
        elif isinstance(other, OperatorsInfo):
            for info in other.operators_info:
                infos.add(info)
        return infos

    def __setitem__(self, key: str, value: Any) -> None:
        self.__setattr__(key, value)

    def update(self, **kwargs):
        for key, value in kwargs.values():
            self[key] = value


class OperatorsInfo:
    def __init__(self):
        self._operators_info: list[OperatorInfo] = []

    def __repr__(self):
        lines = [f"{self.__class__.__name__}:"]
        for info in self.operators_info:
            lines.append(f"  {info}")
        return "\n".join(lines)

    def __add__(self, other: Self):
        infos = OperatorsInfo()
        for info in self.operators_info:
            infos.add(info)
        if isinstance(other, OperatorInfo):
            infos.add(other)
        elif isinstance(other, OperatorsInfo):
            for info in other.operators_info:
                infos.add(info)
        return infos

    def __getitem__(self, index: int) -> OperatorInfo | None:
        return self.operators_info[index]

    def get(self, index: int) -> OperatorInfo | None:
        try:
            return self.operators_info[index]
        except IndexError:
            return

    @property
    def operators_info(self):
        return self._operators_info

    def add(self, info: OperatorInfo | Self):
        if isinstance(info, OperatorInfo):
            self._operators_info.append(info)
        elif isinstance(info, OperatorsInfo):
            for inf in info.operators_info:
                self._operators_info.append(inf)
        else:
            raise TypeError(f"Invalid type {type(info)} for {info}")

    @property
    def all_is_valid(self) -> bool:
        for info in self._operators_info:
            if not info.valid:
                return False
        return True

    @property
    def all_succeeded(self) -> bool:
        for info in self._operators_info:
            if not info.success:
                return False
        return True

    @property
    def cause_for_termination(self) -> bool:
        for info in self._operators_info:
            if info.cause_for_termination:
                return True
        return False


def get_single_operators_info(
    operator_info: OperatorInfo | None = None,
    operator: Operator = None,
    valid: bool = True,
    success: bool = True,
    exception: Exception | None = None,
    msg: str = "",
    cause_for_termination: bool = False,
    **kwargs,
):
    """Returns a OperatorsInfo object containing one OperatorInfo object"""
    all_info = OperatorsInfo()
    if not operator_info:
        operator_info = OperatorInfo(
            operator=operator,
            valid=valid,
            success=success,
            exception=exception,
            msg=msg,
            cause_for_termination=cause_for_termination,
        )
    if not isinstance(operator_info, OperatorInfo):
        raise TypeError(
            f"Invalid type {type(operator_info)} for operator_info object {operator_info}"
        )
    all_info.add(operator_info)
    return all_info

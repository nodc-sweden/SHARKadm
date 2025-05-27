import inspect
import logging
import time
from functools import wraps

from sharkadm import event
from sharkadm.sharkadm_logger.base import SharkadmLoggerExporter
from sharkadm.sharkadm_logger.feedback import Feedback

logger = logging.getLogger(__name__)


DATA_DTYPE = list[dict[str, str]]


class SHARKadmLogger:
    """Class to log events etc. in the SHARKadm data model"""

    # Levels
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

    # Log types
    VALIDATION = "validation"
    TRANSFORMATION = "transformation"
    EXPORT = "export"

    WORKFLOW = "workflow"

    # Purposes
    GENERAL = "general"
    FEEDBACK = "feedback"

    _levels = (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    _log_types = (VALIDATION, TRANSFORMATION, EXPORT, WORKFLOW)

    _purposes = (
        GENERAL,
        FEEDBACK,
    )

    def __init__(self):
        self.feedback = Feedback()

        self._data: DATA_DTYPE = []
        self._filtered_data: DATA_DTYPE = []
        self.name: str = ""
        self._nr_log_entries: int = 0
        self._dataset_name: str = ""

        self._filtered_levels: list[str] = []
        self._filtered_purposes: list[str] = []
        self._filtered_log_types: list[str] = []

    def _reset_log(self) -> None:
        self._data: DATA_DTYPE = []
        self._filtered_data: DATA_DTYPE = []
        self.name: str = ""
        self._nr_log_entries: int = 0
        self._dataset_name: str = ""

        self._filtered_levels: list[str] = []
        self._filtered_purposes: list[str] = []
        self._filtered_log_types: list[str] = []

    def _reset_filter(self) -> None:
        self._filtered_data: DATA_DTYPE = []
        self._filtered_levels: list[str] = []
        self._filtered_purposes: list[str] = []
        self._filtered_log_types: list[str] = []

    def _check_level(self, level: str) -> str:
        level = level.lower()
        if level not in self._levels:
            msg = f"Invalid level: {level}"
            logger.error(msg)
            raise KeyError(msg)
        return level

    @property
    def data(self) -> DATA_DTYPE:
        if self._filtered_data:
            return self._filtered_data
        return self._data

    @property
    def keys(self) -> list[str]:
        key_set = set()
        for data in self.data:
            key_set.update(list(data))
        return sorted(key_set)

    @property
    def levels(self) -> list[str]:
        return self._get_unique_col_items("level")

    @property
    def log_types(self) -> list[str]:
        return self._get_unique_col_items("log_types")

    @property
    def purposes(self) -> list[str]:
        return self._get_unique_col_items("purposes")

    def _get_unique_col_items(self, col: str) -> list[str]:
        return sorted(set([line.get(col) for line in self.data]))

    @property
    def filtered_on_levels(self) -> list[str]:
        return self._filtered_levels or self._levels

    @property
    def filtered_on_log_types(self) -> list[str]:
        return self._filtered_purposes or self._log_types

    @property
    def filtered_on_purposes(self) -> list[str]:
        return self._filtered_log_types or self._purposes

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @dataset_name.setter
    def dataset_name(self, dataset_name: str):
        self._dataset_name = str(dataset_name)

    def log_workflow(
        self,
        msg: str,
        level: str = "info",
        purpose: str = "",
        item: str | None = None,
        cls: str | None = None,
    ) -> None:
        if not cls:
            cls = ""
            stack = inspect.stack()
            if stack[1][0].f_locals.get("self"):
                cls = stack[1][0].f_locals["self"].__class__.__name__
        data = dict(
            log_type=self.WORKFLOW,
            msg=msg,
            level=level,
            cls=cls,
            item=item,
            purpose=purpose,
        )
        self._log(**data)

    def log_transformation(
        self,
        msg: str,
        level: str = "info",
        purpose: str = "",
        row_number: int | None = None,
        row_numbers: list[int] | None = None,
        item: str | None = None,
        cls: str | None = None,
    ) -> None:
        if row_number is not None:
            row_numbers = [row_number] + (row_numbers or [])

        data = dict(
            log_type=self.TRANSFORMATION,
            msg=msg,
            level=level,
            cls=cls,
            item=item,
            purpose=purpose,
            row_numbers=row_numbers,
        )
        self._log(**data)

    def log_validation_failed(
        self,
        msg: str,
        level: str = "warning",
        item: str | None = None,
        purpose: str = "",
        validator: str | None = None,
        column: str | None = None,
        row_number: int | None = None,
        row_numbers: list[int] | None = None,
        cls: str | None = None,
    ) -> None:
        if not cls:
            cls = ""
            stack = inspect.stack()
            if stack[1][0].f_locals.get("self"):
                cls = stack[1][0].f_locals["self"].__class__.__name__

        if row_number is not None:
            row_numbers = [row_number] + (row_numbers or [])

        data = dict(
            log_type=self.VALIDATION,
            msg=msg,
            level=level,
            cls=cls,
            item=item,
            purpose=purpose,
            validator=validator,
            column=column,
            row_numbers=row_numbers,
            validation_success=False,
        )
        self._log(**data)

    def log_validation_succeeded(
        self,
        msg: str,
        level: str = "warning",
        item: str | None = None,
        purpose: str = "",
        validator: str | None = None,
        column: str | None = None,
        row_number: int | None = None,
        row_numbers: list[int] | None = None,
        cls: str | None = None,
    ) -> None:
        if not cls:
            cls = ""
            stack = inspect.stack()
            if stack[1][0].f_locals.get("self"):
                cls = stack[1][0].f_locals["self"].__class__.__name__

        if row_number is not None:
            row_numbers = [row_number] + (row_numbers or [])

        data = dict(
            log_type=self.VALIDATION,
            msg=msg,
            level=level,
            cls=cls,
            item=item,
            purpose=purpose,
            validator=validator,
            column=column,
            row_numbers=row_numbers,
            validation_success=True,
        )
        self._log(**data)

    def log_export(
        self,
        msg: str,
        level: str = "info",
        purpose: str = "",
        item: str | None = None,
        cls: str | None = None,
    ) -> None:
        data = dict(
            log_type=self.EXPORT,
            msg=msg,
            level=level,
            cls=cls,
            item=item,
            purpose=purpose,
        )
        self._log(**data)

    def log_time(self, func):
        """https://dev.to/kcdchennai/python-decorator-to-measure-execution-time-54hk"""

        @wraps(func)
        def timeit_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            msg = f"{func.__name__}{args} {kwargs} took {total_time:.4f} seconds"
            self._log(log_type=self.WORKFLOW, msg=msg, level=self.DEBUG)
            return result

        return timeit_wrapper

    def _log(self, **data) -> None:
        data["level"] = self._check_level(data.get("level", self.INFO))
        data["log_type"] = data.get("log_type", self.WORKFLOW)
        data["purpose"] = data.get("purpose", self.GENERAL)

        self._nr_log_entries += 1
        data["log_nr"] = self._nr_log_entries
        data["dataset_name"] = self.dataset_name

        self._data.append(data)

        event.post_event(f"log_{data['log_type'].lower()}", data)
        event.post_event("log", data)

    def reset_log(self) -> "SHARKadmLogger":
        """Resets all entries to the log"""
        logger.info(f"Resetting {self.__class__.__name__}")
        self._reset_log()
        return self

    def reset_filter(self) -> "SHARKadmLogger":
        logger.info(f"Resetting filter in {self.__class__.__name__}")
        self._reset_filter()
        return self

    def export(self, exporter: SharkadmLoggerExporter):
        exporter.export(self)
        return self

    def filter(
        self,
        *args,
        log_type: str | None = None,
        log_types: str | list | None = None,
        level: str | None = None,
        levels: str | list | None = None,
        in_msg: str | None = None,
        **kwargs,
    ) -> "SHARKadmLogger":
        if level and type(level) is str:
            levels = [level]
        if log_type and type(log_type) is str:
            log_types = [log_type]
        self._filter_data(
            *args, log_types=log_types, levels=levels, in_msg=in_msg, **kwargs
        )
        return self

    def _get_levels(self, *args: str, levels: str | list | None = None):
        use_levels = []
        for arg in args:
            if arg.lower().strip("<>") in self._levels:
                use_levels.append(arg.lower())
        if type(levels) is str:
            levels = [levels]
        if levels:
            use_levels = list(set(use_levels + levels))
        use_levels = use_levels or self._levels
        levels_to_use = set()
        for level in use_levels:
            if "<" in level:
                levels_to_use.update(
                    self._levels[: self._levels.index(level.strip("<>")) + 1]
                )
            elif ">" in level:
                levels_to_use.update(
                    self._levels[self._levels.index(level.strip("<>")) :]
                )
            else:
                levels_to_use.add(level)
        return [level for level in self._levels if level in levels_to_use]

    def _get_log_types(self, *args: str, log_types: str | list | None = None):
        use_log_types = []
        for arg in args:
            if arg.lower() in self._log_types:
                use_log_types.append(arg.lower())
        if type(log_types) is str:
            log_types = [log_types]
        if log_types:
            use_log_types = list(set(use_log_types + log_types))
        use_log_types = use_log_types or self._log_types
        return use_log_types

    def _get_purposes(self, *args: str, purposes: str | list | None = None):
        use_purposes = []
        for arg in args:
            if arg.lower() in self._purposes:
                use_purposes.append(arg.lower())
        if type(purposes) is str:
            purposes = [purposes]
        if purposes:
            use_purposes = list(set(use_purposes + purposes))
        use_purposes = use_purposes or self._purposes
        return use_purposes

    def _filter_data(
        self,
        *args,
        log_types: str | list | None = None,
        levels: str | list | None = None,
        purposes: str | list | None = None,
        in_msg: str | None = None,
        **kwargs,
    ) -> None:
        log_types = self._get_log_types(*args, log_types=log_types)
        levels = self._get_levels(*args, levels=levels)
        purposes = self._get_purposes(*args, purposes=purposes)

        print(f"{levels=}")
        self._filtered_levels = levels
        self._filtered_purposes = purposes
        self._filtered_log_types = log_types

        self._filtered_data = []
        for data in self.data:
            if data.get("level") and data.get("level") not in levels:
                continue
            if data.get("purpose") and data.get("purpose") not in purposes:
                continue
            if data.get("log_type") and data.get("log_type") not in log_types:
                continue
            if in_msg and in_msg not in data.get("msg", ""):
                continue
            self._filtered_data.append(data)

    @staticmethod
    def subscribe(ev: str, func, prio: int = 50) -> None:
        event.subscribe(ev, func, prio)

    def print_on_screen(self, *args, **kwargs):
        def _print(data: dict):
            if data["level"] not in levels:
                return
            if data["log_type"] not in log_types:
                return
            if data["purpose"] and data["purpose"] not in purposes:
                return
            print(data)

        log_types = self._get_log_types(*args, log_types=kwargs.get("log_types"))
        levels = self._get_levels(*args, levels=kwargs.get("levels"))
        purposes = self._get_purposes(*args, purposes=kwargs.get("purposes"))
        print(f"{log_types=}")
        print(f"{levels=}")
        print(f"{purposes=}")
        self.subscribe("log", _print)

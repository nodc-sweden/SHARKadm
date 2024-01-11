import datetime
import inspect
import logging
import pandas as pd

from typing import Optional
from sqlmodel import Field, Session, SQLModel, create_engine, select


from SHARKadm import event

logger = logging.getLogger(__name__)


class LogEntry(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime.datetime
    log_type: str
    msg: str
    count: int = Field(default=1)
    level: str = Field(default='info')
    line: Optional[str]
    data_source: Optional[str]
    cls: Optional[str]


class SHARKadmLogger:
    """Class to log events etc. in the SHARKadm data model"""
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

        self._log_types: list[str] = [
            'validations'
        ]

        self._transformations: dict = dict()
        self._validations: dict = dict()
        self._exports: dict = dict()
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
        time_str = str(datetime.datetime.now())
        self._workflow.append((time_str, msg))
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

    def get_log_info(self,
                     log_types: str | None = None,
                     levels: str | list | None = None,
                     ) -> dict:
        if type(levels) == str:
            levels = [levels]
        if type(levels) == str:
            levels = [levels]
        levels = levels or []
        levels = levels or []
        info = dict()
        info['validations'] = self._validations
        info['transformations'] = self._transformations
        info['exports'] = self._exports
        info['workflow'] = self._workflow

        return info

    def get_log_as_text(self):
        length = 120
        lines = []
        lines.append('=' * length)
        lines.append('SHARKadm log')
        lines.append('-'*length)
        info = self.get_log_info()
        for operator_name, operator_data in info.items():
            lines.append('')
            lines.append(operator_name)
            lines.append('-' * length)
            if operator_name == 'workflow':
                for item in operator_data:
                    lines.append(f'{item[0]}: {item[1]}')
            else:
                for level, data in operator_data.items():
                    lines.append(f'   {level}')
                    for item in data:
                        lines.append(f'      {item}')
            lines.append('.' * length)
            lines.append('')
        return '\n'.join(lines)

    def print_log(self):
        print(self.get_log_as_text())


class SHARKadmLoggerSql:
    """Class to log events etc. in the SHARKadm data model"""
    DEBUG = 'debug'
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'

    VALIDATION = 'validation'
    TRANSFORMATION = 'transformation'
    EXPORT = 'export'

    WORKFLOW = 'workflow'

    levels = [
        DEBUG,
        INFO,
        WARNING,
        ERROR
    ]

    log_types = [
        VALIDATION,
        TRANSFORMATION,
        EXPORT
    ]

    _data = pd.DataFrame()
    _data_columns = ['timestamp', 'log_type', 'msg', 'count', 'level', 'line', 'data_source', 'cls']
    _has_been_transformed = False

    def __init__(self):
        self._reset_log()

    @property
    def timestamp(self):
        return datetime.datetime.now()

    def _reset_log(self):
        logger.info(f'Resetting {self.__class__.__name__}')

        self.engine = create_engine("sqlite:///sharkadm_log.db")

        SQLModel.metadata.create_all(self.engine)

        # self._data = pd.DataFrame(columns=self._data_columns)
        # self._has_been_transformed = False

    def _get_log_type(self, *args: str) -> str | None:
        for arg in args:
            for oper in self.log_types:
                if oper in arg.lower() or arg.lower() in oper:
                    return oper

    def _get_level(self, *args: str) -> str | None:
        for arg in args:
            for lev in self.levels:
                if arg.lower() in lev or lev in arg.lower():
                    return lev

    def log_workflow(self, msg: str, level: str = 'info') -> None:
        stack = inspect.stack()
        cls = stack[1][0].f_locals['self'].__class__.__name__
        self.log(msg, 'workflow', level, cls=cls)

    def log_transformation(self, msg: str, level: str = 'info', as_duplicate=False, **kwargs) -> None:
        stack = inspect.stack()
        cls = stack[1][0].f_locals['self'].__class__.__name__
        self.log(msg, 'transform', level, cls=cls, as_duplicate=as_duplicate, **kwargs)

    def log_validation(self, msg: str, level: str = 'info', as_duplicate=False, **kwargs) -> None:
        stack = inspect.stack()
        cls = stack[1][0].f_locals['self'].__class__.__name__
        self.log(msg, 'validate', level, cls=cls, as_duplicate=as_duplicate, **kwargs)

    def log_exports(self, msg: str, level: str = 'info', **kwargs) -> None:
        stack = inspect.stack()
        cls = stack[1][0].f_locals['self'].__class__.__name__
        self.log(msg, 'export', level, cls=cls, **kwargs)

    def log(self, msg: str, *args: str, **kwargs):
        stack = inspect.stack()
        if 'self' in stack[1][0].f_locals:
            kwargs['cls'] = stack[1][0].f_locals['self'].__class__.__name__
        log_type = self._get_log_type(*args) or self.WORKFLOW
        level = self._get_level(*args) or self.INFO
        self._add_to_log(
            msg=msg,
            log_type=log_type,
            level=level,
            **kwargs
        )

    def _add_to_log(self, **kwargs):
        kwargs['timestamp'] = self.timestamp
        kwargs.pop('count', None)
        kwargs['source_class'] = kwargs.pop('cls', None)
        as_duplicate = kwargs.pop('as_duplicate', False)  # This will increment count if all info is the same
        kw = self._filter_kwargs(**kwargs)
        self._increment_count(**kw)
        # if as_duplicate:
        #     self._increment_count(**kw)
        # else:
        #     self._add_new_log(**kw)

    def _filter_kwargs(self, **kwargs):
        kw = dict()
        for key, value in kwargs.items():
            if key not in self._data_columns:
                continue
            kw[key] = value
        return kw

    def _increment_count(self, **kwargs):
        """This will increment count in an existing log if data in kwargs is the same."""
        with Session(self.engine) as session:
            statement = select(LogEntry)
            for key, value in kwargs.items():
                if key == 'timestamp':
                    continue
                statement = statement.where(getattr(LogEntry, key) == value)
            # statement = select(LogEntry).where(LogEntry.name == "Spider-Boy")
            results = session.exec(statement)
            all_logs = results.fetchall()
            # print(f'{len(all_logs)=}')
            # log = results.one()
            if not all_logs:
                self._add_new_log(**kwargs)
                return
            log = all_logs[-1]
            log.count += 1
            session.add(log)
            session.commit()
            session.refresh(log)

        # boolean = pd.array([True for _ in range(len(adm_logger._data))], dtype="boolean")
        # for key, value in kwargs.items():
        #     boolean = boolean & self._data[key] == value
        # index = list(self._data.index[boolean])
        # if not index:
        #     self._add_new_log(**kwargs)
        #     return
        # self._data.iloc[index[-1], 'count'] += 1

    def _add_new_log(self, **kwargs):
        kwargs['count'] = 1
        entry = LogEntry(**kwargs)
        with Session(self.engine) as session:
            session.add(entry)
            session.commit()

        # self._data.loc[len(self._data)] = kwargs

    def get_log_info(self,
                     log_types: str | None = None,
                     levels: str | list | None = None,
                     ) -> dict:
        if type(levels) == str:
            levels = [levels]
        if type(levels) == str:
            levels = [levels]
        levels = levels or []
        levels = levels or []
        info = dict()
        info['validations'] = self._validations
        info['transformations'] = self._transformations
        info['exports'] = self._exports
        info['workflow'] = self._workflow

        return info

    def get_log_as_text(self):
        length = 120
        lines = []
        lines.append('=' * length)
        lines.append('SHARKadm log')
        lines.append('-' * length)
        info = self.get_log_info()
        for operator_name, operator_data in info.items():
            lines.append('')
            lines.append(operator_name)
            lines.append('-' * length)
            if operator_name == 'workflow':
                for item in operator_data:
                    lines.append(f'{item[0]}: {item[1]}')
            else:
                for level, data in operator_data.items():
                    lines.append(f'   {level}')
                    for item in data:
                        lines.append(f'      {item}')
            lines.append('.' * length)
            lines.append('')
        return '\n'.join(lines)

    def print_log(self):
        print(self.get_log_as_text())


class SHARKadmLoggerDict:
    """Class to log events etc. in the SHARKadm data model"""
    DEBUG = 'debug'
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'

    VALIDATION = 'validation'
    TRANSFORMATION = 'transformation'
    EXPORT = 'export'

    WORKFLOW = 'workflow'

    levels = [
        DEBUG,
        INFO,
        WARNING,
        ERROR
    ]

    log_types = [
        VALIDATION,
        TRANSFORMATION,
        EXPORT
    ]

    _data = dict()
    _data_items = ['msg', 'log_type', 'level', 'data_source', 'data_line', 'source_class']

    def __init__(self):
        self._reset_log()

    def _reset_log(self):
        logger.info(f'Resetting {self.__class__.__name__}')

        self._data = dict()

    def _get_log_type(self, *args: str) -> str | None:
        for arg in args:
            for oper in self.log_types:
                if oper in arg.lower() or arg.lower() in oper:
                    return oper

    def _get_level(self, *args: str) -> str | None:
        for arg in args:
            for lev in self.levels:
                if arg.lower() in lev or lev in arg.lower():
                    return lev

    def log_workflow(self, msg: str, level: str = 'info') -> None:
        stack = inspect.stack()
        source_class = ''
        if 'self' in stack[1][0].f_locals:
            source_class = stack[1][0].f_locals['self'].__class__.__name__
        self.log(msg, 'workflow', level, source_class=source_class)

    def log_transformation(self, msg: str, level: str = 'info', **kwargs) -> None:
        stack = inspect.stack()
        source_class = ''
        if 'self' in stack[1][0].f_locals:
            source_class = stack[1][0].f_locals['self'].__class__.__name__
        self.log(msg, 'transform', level, source_class=source_class, **kwargs)

    def log_validation(self, msg: str, level: str = 'info', **kwargs) -> None:
        stack = inspect.stack()
        source_class = ''
        if 'self' in stack[1][0].f_locals:
            source_class = stack[1][0].f_locals['self'].__class__.__name__
        self.log(msg, 'validate', level, source_class=source_class, **kwargs)

    def log_exports(self, msg: str, level: str = 'info', **kwargs) -> None:
        stack = inspect.stack()
        source_class = ''
        if 'self' in stack[1][0].f_locals:
            source_class = stack[1][0].f_locals['self'].__class__.__name__
        self.log(msg, 'export', level, source_class=source_class, **kwargs)

    def log(self, msg: str, *args: str, **kwargs):
        stack = inspect.stack()
        if 'self' in stack[1][0].f_locals:
            kwargs['source_class'] = stack[1][0].f_locals['self'].__class__.__name__
        log_type = self._get_log_type(*args) or self.WORKFLOW
        level = self._get_level(*args) or self.INFO
        self._add_to_log(
            msg=msg,
            log_type=log_type,
            level=level,
            **kwargs
        )

    @property
    def _timestamp(self):
        return datetime.datetime.now()

    @property
    def _log_template(self):
        return dict(
            timestamp=self._timestamp,
            count=0
        )

    def _get_log_key(self, **kwargs):
        key_list = []
        for item in self._data_items:
            key_list.append(kwargs.get(item, ''))
        return tuple(key_list)

    def _add_to_log(self, **kwargs):
        kw = self._filter_kwargs(**kwargs)
        key = self._get_log_key(**kw)
        self._data.setdefault(key, self._log_template)
        self._data[key]['count'] += 1
        self._data[key]['timestamp'] = self._timestamp

    def _filter_kwargs(self, **kwargs):
        kw = dict()
        for key, value in kwargs.items():
            if key not in self._data_items:
                continue
            kw[key] = value
        return kw

    def get_log_info(self,
                     log_types: str | None = None,
                     levels: str | list | None = None,
                     ) -> dict:
        if type(levels) == str:
            levels = [levels]
        if type(levels) == str:
            levels = [levels]
        levels = levels or []
        levels = levels or []
        info = dict()
        info['validations'] = self._validations
        info['transformations'] = self._transformations
        info['exports'] = self._exports
        info['workflow'] = self._workflow

        return info

    def get_log_as_text(self):
        length = 120
        lines = []
        lines.append('=' * length)
        lines.append('SHARKadm log')
        lines.append('-' * length)
        info = self.get_log_info()
        for operator_name, operator_data in info.items():
            lines.append('')
            lines.append(operator_name)
            lines.append('-' * length)
            if operator_name == 'workflow':
                for item in operator_data:
                    lines.append(f'{item[0]}: {item[1]}')
            else:
                for level, data in operator_data.items():
                    lines.append(f'   {level}')
                    for item in data:
                        lines.append(f'      {item}')
            lines.append('.' * length)
            lines.append('')
        return '\n'.join(lines)

    def print_log(self):
        print(self.get_log_as_text())


# stack = inspect.stack()
# the_class = stack[1][0].f_locals['self'].__class__.__name__
# the_method = stack[1][0].f_code.co_name
# print("I was called by {}.{}()".format(the_class, the_method))


adm_logger = SHARKadmLogger()
# adm_logger = SHARKadmLoggerDict()

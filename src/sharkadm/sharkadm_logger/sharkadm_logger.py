import datetime
import inspect
import logging
import pathlib
import time
from functools import wraps

import pandas as pd

from typing import Optional
# from sqlmodel import Field, Session, SQLModel, create_engine, select


from sharkadm import event
from sharkadm import utils
from sharkadm.sharkadm_logger.exporter import SharkadmExporter
from sharkadm.sharkadm_logger.feedback import Feedback

logger = logging.getLogger(__name__)


class SHARKadmLogger:
    """Class to log events etc. in the SHARKadm data model"""

    # Levels
    DEBUG = 'debug'
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'
    CRITICAL = 'critical'

    # Log types
    VALIDATION = 'validation'
    TRANSFORMATION = 'transformation'
    EXPORT = 'export'

    WORKFLOW = 'workflow'

    # Purposes
    GENERAL = 'general'
    FEEDBACK = 'feedback'

    _levels = [
        DEBUG,
        INFO,
        WARNING,
        ERROR,
        CRITICAL
    ]

    _log_types = [
        VALIDATION,
        TRANSFORMATION,
        EXPORT,
        WORKFLOW
    ]

    _purposes = [
        GENERAL,
        FEEDBACK,
    ]

    _data = dict()

    def __init__(self):
        self.feedback = Feedback()
        self._initiate_log()

    def _initiate_log(self) -> None:
        self._data = dict((lev, {}) for lev in self._levels)
        self._filtered_data = dict()
        self.name = ''
        self._nr_log_entries = 0
        self._dataset_name = ''

    def _check_level(self, level: str) -> str:
        level = level.lower()
        if level not in self._levels:
            msg = f'Invalid level: {level}'
            logger.error(msg)
            raise KeyError(msg)
        return level

    @property
    def data(self) -> dict:
        if self._filtered_data:
            return self._filtered_data
        return self._data

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @dataset_name.setter
    def dataset_name(self, dataset_name: str):
        self._dataset_name = str(dataset_name)

    def log_workflow(self,
                     msg: str,
                     level: str = 'info',
                     purpose: str = '',
                     add: str | None = None) -> None:
        cls = ''
        stack = inspect.stack()
        if stack[1][0].f_locals.get('self'):
            cls = stack[1][0].f_locals["self"].__class__.__name__
            # the_method = stack[1][0].f_code.co_name
            # msg = f'{the_class}: {msg}'
            # print(f'{the_class=}')
            # print(f'{the_method=}')
        self.log(log_type=self.WORKFLOW, msg=msg, level=level, cls=cls, add=add, purpose=purpose)
        # time_str = str(datetime.datetime.now())
        # self._workflow.append((time_str, msg))
        event.post_event('log_workflow', msg)

    def log_transformation(self,
                           msg: str,
                           level: str = 'info',
                           purpose: str = '',
                           row_number: str | int | None = None,
                           add: str | None = None) -> None:
        cls = ''
        stack = inspect.stack()
        if stack[1][0].f_locals.get('self'):
            cls = stack[1][0].f_locals["self"].__class__.__name__
        self.log(log_type=self.TRANSFORMATION, msg=msg, level=level, cls=cls, add=add, purpose=purpose, row_number=row_number)
        # level = self._check_level(level)
        # self._transformations[level].setdefault(msg, 0)
        # self._transformations[level][msg] += 1
        event.post_event('log_transformation', msg)

    def log_validation(self,
                       msg: str,
                       level: str = 'warning',
                       add: str | None = None,
                       purpose: str = '',
                       row_number: str | int | None = None) -> None:
        cls = ''
        stack = inspect.stack()
        if stack[1][0].f_locals.get('self'):
            cls = stack[1][0].f_locals["self"].__class__.__name__
        self.log(log_type=self.VALIDATION, msg=msg, level=level, cls=cls, add=add, purpose=purpose, row_number=row_number)
        # level = self._check_level(level)
        # self._validations[level].setdefault(msg, 0)
        # self._validations[level][msg] += 1
        event.post_event('log_validation', msg)

    def log_export(self,
                   msg: str,
                   level: str = 'info',
                   purpose: str = '',
                   add: str | None = None) -> None:
        cls = ''
        stack = inspect.stack()
        if stack[1][0].f_locals.get('self'):
            cls = stack[1][0].f_locals["self"].__class__.__name__
        self.log(log_type=self.EXPORT, msg=msg, level=level, cls=cls, add=add, purpose=purpose)
        # level = self._check_level(level)
        # self._exports[level].setdefault(msg, 0)
        # self._exports[level][msg] += 1
        event.post_event('log_export', msg)

    def log_time(self, func):
        """https://dev.to/kcdchennai/python-decorator-to-measure-execution-time-54hk"""
        @wraps(func)
        def timeit_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            msg = f'{func.__name__}{args} {kwargs} took {total_time:.4f} seconds'
            self.log(log_type=self.WORKFLOW, msg=msg, level=self.DEBUG)
            event.post_event('log_workflow', msg)
            return result

        return timeit_wrapper

    def log(self,
            msg: str,
            level: str = '',
            log_type: str = '',
            add: str | None = None,
            cls: str = '',
            purpose: str = '',
            **kwargs) -> None:

        level = level or self.INFO
        log_type = log_type or self.WORKFLOW
        purpose = purpose or self.GENERAL

        self._nr_log_entries += 1
        level = self._check_level(level)
        self._data[level].setdefault(purpose, dict())
        self._data[level][purpose].setdefault(log_type, dict())
        # self._data[level][log_type].setdefault(msg, dict(count=0, items=[], time=datetime.datetime.now()))
        self._data[level][purpose][log_type].setdefault(msg, dict(count=0, items=[]))
        # self._data[level][purpose].setdefault(msg, 0)
        self._data[level][purpose][log_type][msg]['count'] += 1
        self._data[level][purpose][log_type][msg]['log_nr'] = self._nr_log_entries
        self._data[level][purpose][log_type][msg]['cls'] = cls
        self._data[level][purpose][log_type][msg]['dataset_name'] = self.dataset_name
        item = []
        if add:
            item.append(f'({self._nr_log_entries}) {add}')
        if kwargs.get('row_number'):
            item.append(f"at row {kwargs.get('row_number')}")
        if item:
            self._data[level][purpose][log_type][msg]['items'].append(' '.join(item))
        event.post_event('log', msg)

    def reset_log(self) -> 'SHARKadmLogger':
        """Resets all entries to the log"""
        logger.info(f'Resetting {self.__class__.__name__}')
        self._initiate_log()
        return self

    def reset_filter(self) -> 'SHARKadmLogger':
        self._filtered_data = dict()
        return self

    def export(self, exporter: SharkadmExporter):
        exporter.export(self)
        return self

    def filter(self, *args,
                     log_type: str | None = None,
                     log_types: str | list | None = None,
                     level: str | None = None,
                     levels: str | list | None = None,
                     in_msg: str | None = None,
                     **kwargs) -> 'SHARKadmLogger':
        if level and type(level) is str:
            levels = [level]
        if log_type and type(log_type) is str:
            log_types = [log_type]
        self._filtered_data = self._get_filtered_data(*args,
                                                      log_types=log_types,
                                                      levels=levels,
                                                      in_msg=in_msg,
                                                      **kwargs)
        return self

    def _get_levels(self, *args: str, levels: str | list | None = None):
        use_levels = []
        for arg in args:
            if arg.lower().strip('<>') in self._levels:
                use_levels.append(arg.lower())
        if type(levels) is str:
            levels = [levels]
        if levels:
            use_levels = list(set(use_levels + levels))
        use_levels = use_levels or self._levels
        levels_to_use = set()
        for level in use_levels:
            if '<' in level:
                levels_to_use.update(self._levels[:self._levels.index(level.strip('<>'))+1])
            elif '>' in level:
                levels_to_use.update(self._levels[self._levels.index(level.strip('<>')):])
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

    def _get_filtered_data(self,
                          *args,
                          log_types: str | list | None = None,
                          levels: str | list | None = None,
                          purposes: str | list | None = None,
                          in_msg: str | None = None,
                          **kwargs
                          ) -> dict:
        # if type(log_types) == str:
        #     log_types = [log_types]
        # if type(levels) == str:
        #     levels = [levels]
        # log_types = log_types or self._log_types
        # levels = levels or self._levels

        log_types = self._get_log_types(*args, log_types=log_types)
        levels = self._get_levels(*args, levels=levels)
        purposes = self._get_purposes(*args, purposes=purposes)

        filtered_data = dict()
        for level_name, level_data in self.data.items():
            if level_name not in levels:
                continue
            filtered_data.setdefault(level_name, dict())
            for purpose, purpose_data in level_data.items():
                if purpose not in purposes:
                    continue
                filtered_data[level_name].setdefault(purpose, dict())
                for log_type_name, log_type_data in purpose_data.items():
                    if log_type_name not in log_types:
                        continue
                    filtered_data[level_name][purpose].setdefault(log_type_name, dict())
                    if in_msg:
                        for msg, msg_data in log_type_data.items():
                            if in_msg and in_msg.lower() not in msg.lower():
                                continue
                            filtered_data[level_name][purpose][log_type_name][msg] = msg_data
                    else:
                        filtered_data[level_name][purpose][log_type_name] = log_type_data
        return filtered_data

    # def save_as_xlsx(self, path: str | pathlib.Path | None = None, include_items: bool = False, **kwargs) -> 'SHARKadmLogger':
    #     if not path:
    #         path = utils.get_export_directory() / 'sharkadm_log.xlsx'
    #     data = []
    #     header = ['Level', 'Log type', 'Message', 'Nr', 'Item']
    #     for level, level_data in self.data.items():
    #         for log_type, log_type_data in level_data.items():
    #             for msg, msg_data in log_type_data.items():
    #                 count = msg_data['count']
    #                 line = [level, log_type, msg, count]
    #                 if not include_items or not msg_data['items']:
    #                     line.append('')
    #                     data.append(line)
    #                     continue
    #                 for item in msg_data['items']:
    #                     data.append(line + [item])
    #
    #     df = pd.DataFrame(data=data, columns=header)
    #     df.fillna('', inplace=True)
    #     self._save_as_xlsx_with_table(path, df)
    #     # df.to_excel(str(path), index=False)
    #     logger.info(f'Saving sharkadm log file to {path}')
    #
    #     return self
    #
    # def _save_as_xlsx_with_table(self, path: pathlib.Path, df: pd.DataFrame):
    #     """
    #     https://stackoverflow.com/questions/58326392/how-to-create-excel-table-with-pandas-to-excel
    #     """
    #
    #     # Create a Pandas Excel writer using XlsxWriter as the engine.
    #     writer = pd.ExcelWriter(str(path), engine='xlsxwriter')
    #
    #     # Convert the dataframe to an XlsxWriter Excel object. Turn off the default
    #     # header and index and skip one row to allow us to insert a user defined
    #     # header.
    #     sheet_name = path.stem.split('SHARK_')[-1][:30]
    #     df.to_excel(writer, sheet_name=sheet_name, startrow=1, header=False, index=False)
    #
    #     # Get the xlsxwriter workbook and worksheet objects.
    #     workbook = writer.book
    #     worksheet = writer.sheets[sheet_name]
    #
    #     # Get the dimensions of the dataframe.
    #     (max_row, max_col) = df.shape
    #
    #     # Create a list of column headers, to use in add_table().
    #     column_settings = []
    #     for header in df.columns:
    #         column_settings.append({'header': header})
    #
    #     # Add the table.
    #     worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
    #
    #     # Make the columns wider for clarity.
    #     worksheet.set_column(0, 0, 10)
    #     worksheet.set_column(1, 1, 20)
    #     worksheet.set_column(2, 2, 90)
    #     worksheet.set_column(3, 3, 8)
    #     worksheet.set_column(4, 4, 70)
    #     # worksheet.set_column(0, max_col - 1, 20)
    #     # worksheet.set_column(0, max_col - 1, 20)
    #
    #     # Close the Pandas Excel writer and output the Excel file.
    #     writer.close()
    #
    # def get_log_lines(self, *args: str, include_items: bool = False, sort_log: bool = True) -> list[str]:
    #     """
    #     Option to filter data.
    #     """
    #     if sort_log:
    #         lines = self._get_sorted_log_lines(self.data, include_items=include_items)
    #     else:
    #         lines = self._get_unsorted_log_lines(self.data, include_items=include_items)
    #     return lines
    #
    # def _get_sorted_log_lines(self, data: dict, **kwargs):
    #     lines = []
    #     for level in sorted(data):
    #         level_data = data[level]
    #         for log_type in sorted(level_data):
    #             log_type_data = level_data[log_type]
    #             for msg in sorted(log_type_data):
    #                 msg_data = log_type_data[msg]
    #                 count = msg_data['count']
    #                 items = msg_data['items']
    #                 lines.append(self._get_log_line(level, log_type, msg, count))
    #                 if kwargs.get('include_items'):
    #                     for item in sorted(items):
    #                         lines.append(self._get_log_item_line(item))
    #     return lines
    #
    # def _get_unsorted_log_lines(self, data: dict, **kwargs):
    #     lines = []
    #     for level, level_data in data.items():
    #         for log_type, log_type_data in level_data.items():
    #             for msg, msg_data in log_type_data.items():
    #                 count = msg_data['count']
    #                 items = msg_data['items']
    #                 lines.append(self._get_log_line(level, log_type, msg, count))
    #                 if kwargs.get('include_items'):
    #                     for item in items:
    #                         lines.append(self._get_log_item_line(item))
    #     return lines
    #
    # def _get_log_line(self, level, log_type, msg, count):
    #     line = f'{level.upper()} - {log_type}: {msg}'
    #     if count > 1:
    #         line += f' ({count} times)'
    #     return line
    #
    # def _get_log_item_line(self, item):
    #     return f'    {item}'
    #
    # def get_log_text(self, *args: str, include_items: bool = False, **kwargs) -> str:
    #     lines = self.get_log_lines(*args, include_items=include_items, **kwargs)
    #     return '\n'.join(lines)
    #
    # def print_log(self, *args: str, include_items: bool = False, **kwargs) -> None:
    #     print(self.get_log_text(*args, include_items=include_items, **kwargs))


class old_SHARKadmLogger:
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

    def log_export(self, msg: str, level: str = 'info') -> None:
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

    def log_export(self, msg: str, level: str = 'info', **kwargs) -> None:
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

    def log_export(self, msg: str, level: str = 'info', **kwargs) -> None:
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

# adm_logger = SHARKadmLoggerDict()

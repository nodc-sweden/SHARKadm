# -*- coding: utf-8 -*-


import logging.handlers
import pathlib
import sys
from typing import Callable

# from SHARKadm.gui.tooltip_texts import TooltipTexts, get_tooltip_widget
# from SHARKadm.gui.texts import Texts
# from SHARKadm.gui.colors import Colors

import flet as ft
from flet_core.constrained_control import ConstrainedControl
import yaml

from .widgets import operation
from .widgets import config
from . import gui_event

from SHARKadm import transformers
from SHARKadm import validators
from SHARKadm import exporters
from SHARKadm.controller import SHARKadmController
from SHARKadm.workflow import SHARKadmArchiveWorkflow


logger = logging.getLogger(__name__)

if getattr(sys, 'frozen', False):
    DIRECTORY = pathlib.Path(sys.executable).parent
else:
    DIRECTORY = pathlib.Path(__file__).parent

# TOOLTIP_TEXT = TooltipTexts()
#
# TEXTS = Texts()
# COLORS = Colors()


class SimpleFletApp:
    def __init__(self, log_in_console=False):
        self._log_in_console = log_in_console
        self.page = None
        self._loaded_config = {}
        self._instrument_items = {}
        self._current_source_instrument = None
        self._controller_cls = SHARKadmController
        self._all_lists = []

        self.logging_level = 'DEBUG'
        self.logging_format = '%(asctime)s [%(levelname)10s]    %(pathname)s [%(lineno)d] => %(funcName)s():    %(message)s'
        self.logging_format_stdout = '[%(levelname)10s] %(filename)s: %(funcName)s() [%(lineno)d] %(message)s'
        # self._setup_logger()

        self.app = ft.app(target=self.main)

    @property
    def _log_directory(self):
        path = pathlib.Path(pathlib.Path.home(), 'logs')
        path.mkdir(parents=True, exist_ok=True)
        return path

    def main(self, page: ft.Page):
        self.page = page
        self.page.title = 'SHARKadm light'
        self.page.window_height = 700
        self.page.window_width = 1100
        # self.page.padding = 0
        # self.page.spacing = 0
        self._build()

        # gui_event.subscribe('change_order', self._on_change_order)

    def update_page(self):
        self.page.update()

    def _build(self):
        self._create_controls()
        gui_event.subscribe('save_config', self._on_save_config)
        gui_event.subscribe('load_config', self._on_load_config)
        row = self._get_layout()
        self.page.controls.append(row)
        self.update_page()
        self._initiate_lists()
        self._config_list.update_list()
        return row

    def _create_controls(self) -> None:
        """Entry point for creating controls"""
        self._create_tab_controls()
        self._create_options_controls()
        self._create_config_controls()

    def _create_options_controls(self) -> None:

        self._container_options = ft.Container()

        self._label_current_config_file = ft.Text('Aktuell config-fil:')
        self._current_config_file = ft.Text()

        self._label_current_data_path = ft.Text('Aktuell data:')
        self._current_data_path = ft.Text()

        self._button_pick_config_file = self._get_pick_config_file_button()
        # self._button_save_as_config_file = self._get_save_as_config_file_button()
        self._button_load_data_directory = self._get_load_directory_file_button()

        self._button_run_workflow = ft.ElevatedButton('Starta workflow', on_click=self._on_start_workflow)
        
    def _create_config_controls(self) -> None:
        self._container_config = ft.Container()
        self._config_list = config.Configs()

    def _on_start_workflow(self, e):
        if not self._current_data_path.value:
            print('Ingen data vald')
            return
        self._start_workflow_based_on_current_view()

    def _start_workflow_based_on_current_view(self) -> None:
        workflow = SHARKadmArchiveWorkflow(
            archive_paths=[self._current_data_path.value],
            validators_before=self._list_validate_before.get_active_data(),
            validators_after=self._list_validate_after.get_active_data(),
            transformers=self._list_transform.get_active_data(),
            exporters=self._list_export.get_active_data())
        workflow.start_workflow()

    def _on_pick_config_file(self, e: ft.FilePickerResultEvent) -> None:
        if not e.files:
            return
        path = e.files[0].path
        self._load_config_file(path)
        self._set_current_config_file_text(path)
        self._update_lists_from_config()

    def _on_load_config(self, data):
        path = str(data['file_path'])
        self._load_config_file(path)
        self._set_current_config_file_text(path)
        self._update_lists_from_config()

    def _on_save_config(self, data):
        self._create_config_file(data['file_path'], data.get('overwrite', False))

    def _create_config_file(self, path: str | pathlib.Path, overwrite=False):
        path = pathlib.Path(path)
        data = dict(archive_paths=[])
        if self._current_data_path.value:
            data['archive_paths'] = [self._current_data_path.value]
        data['validators_before'] = self._get_config_list(self._list_validate_before.get_active_data())
        data['validators_after'] = self._get_config_list(self._list_validate_after.get_active_data())
        data['transformers'] = self._get_config_list(self._list_transform.get_active_data())
        data['exporters'] = self._get_config_list(self._list_export.get_active_data())

        if path.exists() and not overwrite:
            logger.warning(f'Overwrite not allowed. Will not create file: {path}')
            return
        with open(path, 'w') as fid:
            yaml.safe_dump(data, fid)
        self._current_config_file.value = str(path)
        self._current_config_file.update()

    @staticmethod
    def _get_config_list(active_data):
        lst = []
        for item in active_data:
            data = dict(name=item['name'])
            if item.get('kwargs'):
                data['kwargs'] = item.get('kwargs')
            lst.append(data)
        return lst

    def _set_current_config_file_text(self, path: str) -> None:
        self._current_config_file.value = path
        self._current_config_file.update()

    def _on_load_data_directory(self, e: ft.FilePickerResultEvent) -> None:
        if not e.path:
            return
        self._current_data_path.value = e.path
        self._current_data_path.update()

    def _load_config_file(self, path: str) -> None:
        with open(path) as fid:
            self._loaded_config = yaml.safe_load(fid)

    def _update_lists_from_config(self) -> None:
        self._update_list_from_config('validators_before',
                                      self._controller_cls.get_validator_list,
                                      self._list_validate_before)
        self._update_list_from_config('transformers',
                                      self._controller_cls.get_transformer_list,
                                      self._list_transform)
        self._update_list_from_config('validators_after',
                                      self._controller_cls.get_validator_list,
                                      self._list_validate_after)
        self._update_list_from_config('exporters',
                                      self._controller_cls.get_exporter_list,
                                      self._list_export)

    def _update_list_from_config(self,
                                 list_name: str,
                                 controller_func: Callable,
                                 list_object: operation.OperationList) -> None:
        # print(f'{list_name=}')
        start_list = self._loaded_config.get(list_name, []) or []
        # print(f'{start_list=}')
        list_to_set = controller_func(start_with=start_list)
        # print()
        # print()
        # print()
        # print()
        # print(f'{list_to_set=}')
        list_object.set_order(list_to_set)
        # print(f'{start_list=}')
        activate_list = [item['name'] for item in start_list]
        list_object.activate(*activate_list, deactivate_others=True)

    def _get_pick_config_file_button(self) -> ft.Row:
        pick_config_file_dialog = ft.FilePicker(on_result=self._on_pick_config_file)

        self.page.overlay.append(pick_config_file_dialog)

        row = ft.Row(
                [
                    ft.ElevatedButton(
                        "Välj en config-fil",
                        icon=ft.icons.UPLOAD_FILE,
                        on_click=lambda _: pick_config_file_dialog.pick_files(
                            allow_multiple=False,
                            allowed_extensions=['yaml']
                        ),
                    ),
                ]
            )
        return row

    # def _get_save_as_config_file_button(self) -> ft.Row:
    #     save_as_config_file_dialog = ft.FilePicker(on_result=self._on_save_as_config_file)
    #
    #     self.page.overlay.append(save_as_config_file_dialog)
    #
    #     row = ft.Row(
    #         [
    #             ft.ElevatedButton(
    #                 "Spara config-fil",
    #                 icon=ft.icons.SAVE,
    #                 on_click=lambda _: save_as_config_file_dialog.save_file(
    #                     'Välj yaml-fil',
    #                     allowed_extensions=['yaml']
    #                 ),
    #             ),
    #         ]
    #     )
    #     return row

    def _get_load_directory_file_button(self) -> ft.Row:
        load_data_directory_file_dialog = ft.FilePicker(on_result=self._on_load_data_directory)

        self.page.overlay.append(load_data_directory_file_dialog)

        row = ft.Row(
            [
                ft.ElevatedButton(
                    "Välj en mapp med data",
                    icon=ft.icons.STORAGE,
                    on_click=lambda _: load_data_directory_file_dialog.get_directory_path(
                        'Välj en mapp med data',
                    ),
                ),
            ]
        )
        return row

    def _create_tab_controls(self) -> None:
        self._tabs = ft.Tabs(
            selected_index=0,
            animation_duration=300,
            expand=True
        )

        self._tab_validate_before = ft.Tab(text="Validera före")
        self._tab_transform = ft.Tab(text="Transformera")
        self._tab_validate_after = ft.Tab(text="Validera efter")
        self._tab_export = ft.Tab(text="Exportera")

        self._container_validate_before = ft.Container()
        self._container_transform = ft.Container()
        self._container_validate_after = ft.Container()
        self._container_export = ft.Container()

        self._list_validate_before = operation.OperationList('validators_before')
        self._list_transform = operation.OperationList('transformers')
        self._list_validate_after = operation.OperationList('validators_after')
        self._list_export = operation.OperationList('exporters')

        self._all_lists = [
            self._list_validate_before,
            self._list_transform,
            self._list_validate_after,
            self._list_export,
        ]

        self._content_validate_before = self._list_validate_before
        self._content_transform = self._list_transform
        self._content_validate_after = self._list_validate_after
        self._content_export = self._list_export

    def _get_layout(self) -> ft.Column:
        col = ft.Column(expand=True)
        row = ft.Row([
            self._get_options_layout(),
            self._get_config_layout()
        ], expand=True)
        col.controls.append(row)
        col.controls.append(self._get_tabs_layout())
        return col

    def _get_tabs_layout(self) -> ft.Tabs:
        self._container_validate_before.content = self._content_validate_before
        self._container_transform.content = self._content_transform
        self._container_validate_after.content = self._content_validate_after
        self._container_export.content = self._content_export

        self._tab_validate_before.content = self._container_validate_before
        self._tab_transform.content = self._container_transform
        self._tab_validate_after.content = self._container_validate_after
        self._tab_export.content = self._container_export

        self._tabs.tabs = [
            self._tab_validate_before,
            self._tab_transform,
            self._tab_validate_after,
            self._tab_export,
        ]
        return self._tabs

    def _get_options_layout(self) -> ft.Row:

        current_config_file_row = ft.Row([
            self._label_current_config_file,
            self._current_config_file
        ])

        current_data_row = ft.Row([
            self._label_current_data_path,
            self._current_data_path
        ])

        inner = ft.Column(expand=True)
        inner.controls.append(current_config_file_row)
        inner.controls.append(self._button_pick_config_file)
        # inner.controls.append(self._button_save_as_config_file)
        inner.controls.append(self._button_load_data_directory)
        inner.controls.append(current_data_row)
        inner.controls.append(self._button_run_workflow)
        self._container_options.content = inner

        row = ft.Row(expand=True)
        row.controls.append(self._container_options)
        return row
    
    def _get_config_layout(self) -> ft.Row:
        self._container_config.content = self._config_list
        return ft.Row([
            self._container_config
        ], expand=True)

    def _initiate_lists(self) -> None:
        self._list_validate_before.create_list_items(self._controller_cls.get_validators())
        self._list_transform.create_list_items(self._controller_cls.get_transformers())
        self._list_validate_after.create_list_items(self._controller_cls.get_validators())
        self._list_export.create_list_items(self._controller_cls.get_exporters())

        # self._list_validate_before.set_list(validators.get_validators_description())
        # self._list_transform.set_list(transformers.get_transformers_description())
        # self._list_validate_after.set_list(validators.get_validators_description())
        # self._list_export.set_list(exporters.get_exporters_description())

    def _on_change_order(self, data: dict):
        order = data.get('active_order')
        uid = data.get('uid')
        changed = []
        for lst in self._all_lists:
            if lst.uid == uid:
                if order == self._get_order_from_config(lst.title):
                    self._current_config_file.bgcolor = ft.colors.RED
                    self._current_config_file.update()
                else:
                    self._current_config_file.bgcolor = ft.colors.BLACK
                    self._current_config_file.update()

    def _get_order_from_config(self, list_name: str):
        print(f'{list_name=}')
        return [item['name'] for item in self._loaded_config.get(list_name, []) or []]



def main(log_in_console):
    app = SimpleFletApp(log_in_console=log_in_console)
    return app
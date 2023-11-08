# -*- coding: utf-8 -*-

import pathlib

import flet as ft
import textwrap

from .. import gui_event
from SHARKadm import utils


class Configs(ft.UserControl):
    def __init__(self, **kwargs):
        super().__init__()
        self._data = kwargs

    def build(self):
        self._create_controls()
        return self._get_layout()

    @property
    def _config_names(self):
        return [cont.text for cont in self._config_list.controls]

    def _create_controls(self):
        self._config_list = ft.Column(scroll=ft.ScrollMode.ALWAYS)
        self._add_button = ft.ElevatedButton('Spara nuvarande configuration', on_click=self._add_config)
        self._add_name = ft.TextField(hint_text='Namn på configfil', suffix_text='.yaml')

    def _get_layout(self):
        col = ft.Column(expand=True)
        add_row = ft.Row([
            self._add_name,
            self._add_button
        ])
        col.controls.append(add_row)
        col.controls.append(self._config_list)
        return col

    def _list_config_files(self):
        self._config_list.controls = []
        for path in sorted(utils.get_config_directory().iterdir()):
            if path.suffix != '.yaml':
                continue
            self._config_list.controls.append(ft.ElevatedButton(path.stem, on_click=lambda x, p=path: self._load_config(
                x, p)))

    def update_list(self):
        self._list_config_files()
        self._config_list.update()

    def _load_config(self, e: ft.ControlEvent, path: pathlib.Path):
        gui_event.post_event('load_config', dict(file_path=path))

    def _add_config(self, e: ft.ControlEvent):
        name = self._add_name.value.strip()
        if not name:
            print('No name given')
            return
        if name in self._config_names:
            print(f'Name {name} exist. Can not overwrite!')
            return
        path = pathlib.Path(utils.get_config_directory() / f'{name}.yaml')
        gui_event.post_event('save_config', dict(
            file_path=path,
        ))
        self._add_name.value = ''
        self._add_name.update()
        self.update_list()








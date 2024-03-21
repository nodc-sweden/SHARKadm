# -*- coding: utf-8 -*-

import pathlib

import flet as ft
import textwrap

from .. import gui_event
from sharkadm import utils


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
        self._add_button = ft.ElevatedButton('Spara nuvarande configuration', on_click=self._try_save_current_config)
        self._add_name = ft.TextField(hint_text='Namn på ny configfil', suffix_text='.yaml')
        self._create_ask_overwrite_dialog()

    def _create_ask_overwrite_dialog(self) -> None:
        self._ask_overwrite_dialog = ft.AlertDialog(
            modal=True,
            title=ft.Text("Bekräfta"),
            content=ft.Text("Det finns redan en konfiguration med det här namnet. Vill du skiva över den befintliga?"),
            actions=[
                ft.TextButton("Ja", on_click=self._on_answer_overwrite_yes),
                ft.TextButton("Nej", on_click=self._on_answer_overwrite_no),
            ],
            actions_alignment=ft.MainAxisAlignment.END,
            # on_dismiss=lambda e: print("Modal dialog dismissed!"),
        )

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

    def _try_save_current_config(self, e: ft.ControlEvent):
        name = self._add_name.value.strip()
        if not name:
            print('No name given')
            return
        if name in self._config_names:
            self._open_ask_overwrite_dialog()
        else:
            self._save_current_config(False)

    def _save_current_config(self, overwrite: bool = False) -> None:
        name = self._add_name.value.strip()
        path = pathlib.Path(utils.get_config_directory() / f'{name}.yaml')
        gui_event.post_event('save_config', dict(
            file_path=path,
            overwrite=overwrite
        ))
        self._add_name.value = ''
        self._add_name.update()
        self.update_list()

    def _open_ask_overwrite_dialog(self):
        self.page.dialog = self._ask_overwrite_dialog
        self._ask_overwrite_dialog.open = True
        self.page.update()

    def _on_answer_overwrite_yes(self, e):
        self._save_current_config(True)
        self._close_ask_overwrite_dialog()

    def _on_answer_overwrite_no(self, e):
        self._close_ask_overwrite_dialog()

    def _close_ask_overwrite_dialog(self):
        self._ask_overwrite_dialog.open = False
        self.page.update()









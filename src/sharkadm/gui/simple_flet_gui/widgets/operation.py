# -*- coding: utf-8 -*-
import pathlib

import flet as ft
import textwrap

from .. import gui_event
from sharkadm import utils


class Operation(ft.UserControl):
    def __init__(self, **kwargs):
        super().__init__()
        self._data = kwargs
        print(f'{self._data=}')
        self._settings_controls = {}

        self._description_wrap_length = 80

        self._main_active_bgcolor = ft.colors.GREEN_50
        self._switch_active_bgcolor = ft.colors.GREEN_100
        self._text_active_bgcolor = ft.colors.GREEN_200

        self._main_inactive_bgcolor = ft.colors.GREY_200
        self._switch_inactive_bgcolor = ft.colors.GREY_200
        self._text_inactive_bgcolor = ft.colors.GREY_300

        self._main_padding = 5
        self._main_border_radius = 10

        self._child_padding = 5
        self._child_border_radius = 10

    @property
    def name(self) -> str:
        return self._data['name']

    @property
    def description(self) -> str:
        return textwrap.fill(self._data.get('description') or '', self._description_wrap_length)

    @property
    def settings(self) -> dict:
        data = self._data.get('kwargs', {})
        for key, control in self._settings_controls.items():
            value = control.value
            if type(value) == pathlib.Path:
                value = str(value)
            data[key] = value
        return data

    @property
    def state(self) -> bool:
        return self._switch.value

    @state.setter
    def state(self, st) -> None:
        assert type(st) == bool
        self._switch.value = st
        self._on_change()

    def _on_change(self, e: ft.ControlEvent | None = None):
        if self._switch.value:
            self._on_select()
        else:
            self._on_deselect()
        gui_event.post_event('change_operator_state', {})
        self.update()

    def _on_select(self):
        self._main_container.bgcolor = self._main_active_bgcolor
        self._switch_container.bgcolor = self._switch_active_bgcolor
        self._text_container.bgcolor = self._text_active_bgcolor

    def _on_deselect(self):
        self._main_container.bgcolor = self._main_inactive_bgcolor
        self._switch_container.bgcolor = self._switch_inactive_bgcolor
        self._text_container.bgcolor = self._text_inactive_bgcolor

    def build(self):
        self._create_controls()
        return self._get_layout()

    def _create_controls(self):
        self._create_settings_dialog()

        self._main_container = ft.Container(
            bgcolor=self._main_inactive_bgcolor,
            padding=self._main_padding,
            border_radius=self._main_border_radius
        )
        self._switch_container = ft.Container(
            bgcolor=self._switch_inactive_bgcolor,
            padding=self._child_padding,
            border_radius=self._child_border_radius
        )
        self._text_container = ft.Container(
            bgcolor=self._text_inactive_bgcolor,
            padding=self._child_padding,
            border_radius=self._child_border_radius
        )
        self._switch = ft.Switch(label=self.name, on_change=self._on_change)
        self._text = ft.Text(self.description, expand=True)

        self._settings_icon_button = ft.IconButton(
                    icon=ft.icons.SETTINGS,
                    on_click=self._open_settings,
                    # icon_color="blue400",
                    icon_size=20,
                    tooltip="Inställningar", visible=False)
        if self.settings:
            self._settings_icon_button.visible = True

        self._child_row = ft.Row()

    def _create_settings_dialog(self):
        self._settings_content = ft.Row()
        self._settings_dialog = ft.AlertDialog(
                modal=True,
                title=ft.Text("Inställningar"),
                content=self._settings_content,
                actions=[
                    ft.TextButton("OK", on_click=self._close_settings),
                ],
                actions_alignment=ft.MainAxisAlignment.END,
                on_dismiss=lambda e: print("Modal dialog dismissed!"),
            )

    def _get_layout(self):
        """Creates the layout and returns the root widget"""

        # self.expand = True
        # self._main_container.expand = True
        # self._child_row.expand = True
        # self._switch_container.expand = True
        # self._text_container.expand = True
        # self._switch.expand = True
        # self._text.expand = True

        self._child_row.alignment = ft.MainAxisAlignment.SPACE_BETWEEN

        row = ft.Row([self._switch, self._settings_icon_button])
        self._switch_container.content = row
        self._text_container.content = self._text

        self._child_row.controls.append(self._switch_container)
        self._child_row.controls.append(self._text_container)

        self._main_container.content = self._child_row
        return self._main_container

    def _open_settings(self, *args):
        self.page.dialog = self._settings_dialog
        self._settings_dialog.open = True
        self.page.update()
        self._update_settings_content(**self.settings)

    def _close_settings(self, *args):
        self._settings_dialog.open = False
        self._settings_dialog.update()

    def _on_pick_directory(self, e: ft.FilePickerResultEvent, linked_control: ft.Text):
        if not e.path:
            return
        linked_control.value = e.path
        linked_control.update()

    def _update_settings_content(self, **kwargs):
        self._settings_content.controls = []
        self._settings_controls = {}
        # column = ft.Column(width=430)
        column = ft.Column(expand=True)
        self._settings_content.controls.append(column)
        for key, value in kwargs.items():
            if key == 'export_directory':
                self._settings_controls[key] = ft.Text()
                pick_file_path = ft.FilePicker(on_result=lambda e, p=self._settings_controls[key]: self._on_pick_directory(e, p))
                self.page.overlay.append(pick_file_path)
                self.page.update()
                button = ft.ElevatedButton(
                            "Välj en mapp att exportera till",
                            icon=ft.icons.UPLOAD_FILE,
                            on_click=lambda _: pick_file_path.get_directory_path(
                            ),
                        )

                row = ft.Row([
                    button,
                    self._settings_controls[key]
                ])
                column.controls.append(row)
                if not value:
                    value = utils.get_export_directory()
            elif key == 'export_file_name':
                self._settings_controls[key] = ft.TextField()
                row = ft.Row([
                    ft.Text('Ange ett filnamn'),
                    self._settings_controls[key]
                ])
                column.controls.append(row)

            elif type(value) == bool:
                self._settings_controls[key] = ft.Switch(label=key)
                row = ft.Row([
                    self._settings_controls[key]
                ])
                column.controls.append(row)
            if key in self._settings_controls:
                self._settings_controls[key].value = value
            # self._settings_controls[key].update()
        self._settings_content.update()

    def set_data(self, **kwargs) -> None:
        self._settings_controls = {}
        self._data = kwargs
        self._set_data()
        self.set_border(None)

    def get_data(self) -> dict:
        return dict(
            name=self.name,
            description=self.description,
            kwargs=self.settings
        )

    def _set_data(self):
        self._switch.label = self.name
        self._text.value = self.description
        self._switch.update()
        self._text.update()

        if self.settings:
            self._settings_icon_button.visible = True
        else:
            self._settings_icon_button.visible = False
        self._settings_icon_button.update()

    def set_border(self, color=None):
        border = None
        if color:
            border = ft.border.only(top=ft.BorderSide(5, color=color))
            # border = ft.border.all(5, color)
        self._main_container.border = border
        self._main_container.update()


class OperationList(ft.UserControl):
    """Inspired by: https://www.youtube.com/watch?v=hHs-IFcBTK8 """

    def __init__(self, title: str):
        super().__init__()
        self._title = title

        self._data = {}
        self._data_order = []
        self._state = {}
        self._list_column = ft.Column(scroll=ft.ScrollMode.ALWAYS)

        self._main_bgcolor = ft.colors.GREEN_50
        self._main_padding = 5
        self._main_border_radius = 10

        self._config_name_order = []

    def build(self):
        self._create_controls()
        gui_event.subscribe('change_operator_state', self._on_change_operation_state)
        return self._get_layout()

    @property
    def title(self) -> str:
        return self._title

    def _create_controls(self):
        self._main_container = ft.Container(
            bgcolor=self._main_bgcolor,
            padding=self._main_padding,
            border_radius=self._main_border_radius,
        )

    def _get_layout(self):
        self.expand = True
        self._main_container.expand = True
        self._list_column.expand = True
        self._main_container.content = self._list_column
        return self._main_container

    def _reset_list(self):
        self._list_column.controls = []

    def _create_list_items(self, sort_list: bool):
        if sort_list:
            keys = sorted(self._data)
        else:
            keys = self._data.keys()
        for key in keys:
            oper = Operation(**self._data[key])
            self._list_column.controls.append(
                ft.DragTarget(
                    data=self._title,
                    group=self._title,
                    on_accept=self._on_drag_accept,
                    on_will_accept=self._on_will_accept,
                    on_leave=self._on_drag_leave,
                    content=ft.Draggable(
                        data=self._title,
                        group=self._title,
                        content=oper,
                    )
                )
            )

    def _on_will_accept(self, e: ft.ControlEvent):
        """Called when drop is accepted"""
        if e.control.content.content.name in self._data_order:
            e.control.content.content.set_border(ft.colors.RED)

    def _on_drag_leave(self, e: ft.ControlEvent):
        e.control.content.content.set_border(None)

    def _on_drag_accept(self, e: ft.DragTargetAcceptEvent):
        self._save_current_data()
        src = self.page.get_control(e.src_id)
        self._update_data_order(src, e.control)
        self._set_list_column()
        self.update()
        self._post_event_on_change_order()

    def _post_event_on_change_order(self) -> None:
        active_order = []
        for cont in self._list_column.controls:
            if cont.content.content.state:
                active_order.append(cont.content.content.name)
        gui_event.post_event('change_order',
                             dict(
                                 uid=self.uid,
                                 order=self._data_order,
                                 active_order=active_order,
                             ))

    def _on_change_operation_state(self, data: dict) -> None:
        self._post_event_on_change_order()

    def _get_index_from_draggable(self, draggable: ft.Draggable) -> int | None:
        for i, cont in enumerate(self._list_column.controls):
            if cont.content.content.name == draggable.content.name:
                return i

    def _get_index_from_drag_target(self, drag_target: ft.DragTarget) -> int | None:
        for i, cont in enumerate(self._list_column.controls):
            if cont.content.content.name == drag_target.content.content.name:
                return i

    def _update_data_order(self, src: ft.Draggable, dest: ft.DragTarget):
        src_index = self._get_index_from_draggable(src)
        dest_index = self._get_index_from_drag_target(dest)
        self._state[src.content.name] = src.content.state
        self._state[dest.content.content.name] = dest.content.content.state
        popped_item = self._data_order.pop(src_index)
        self._data_order.insert(dest_index, popped_item)

    def _set_list_column(self) -> None:
        """Sets the list column based on order in self._data_order"""
        for name, control in zip(self._data_order, self._list_column.controls):
            oper = control.content.content
            oper.set_data(**self._data[name])
            oper.state = self._state.get(name, False)
        self.update()

    def create_list_items(self, data: dict[str, dict], sort_list=False):
        self._data = data.copy()
        self._data_order = list(self._data.keys())
        self._reset_list()
        self._create_list_items(sort_list=sort_list)
        self.update()

    # def _old_create_list_items(self, data: dict, sort_list=False):
    #     self._data = data.copy()
    #     self._data_order = list(self._data.keys())
    #     self._reset_list()
    #     self._create_list_items(sort_list=sort_list)
    #     self.update()

    def set_order(self, new_order: dict[str, dict]):
        # self._save_current_data()
        self._update_data(new_order)
        self._data_order = list(new_order)
        self._set_list_column()

    def _update_data(self, data):
        for key, value in data.items():
            self._data[key].update(value)

    def deactivate_all(self) -> None:
        for control in self._list_column.controls:
            oper = control.content.content
            oper.state = False

    def activate(self, *operations_to_activate: str, deactivate_others: bool = False):
        for control in self._list_column.controls:
            oper = control.content.content
            if oper.name in operations_to_activate:
                oper.state = True
            elif deactivate_others:
                oper.state = False

    def _save_current_data(self) -> None:
        for control in self._list_column.controls:
            oper = control.content.content
            self._data[oper.name].update(**oper.get_data())

    def get_active_data(self) -> list[dict[str, dict]]:
        data = []
        for control in self._list_column.controls:
            oper = control.content.content
            if not oper.state:
                continue
            data.append(oper.get_data())
        return data

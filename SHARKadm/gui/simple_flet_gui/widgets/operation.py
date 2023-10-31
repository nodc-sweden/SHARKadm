import flet as ft
import textwrap


class Operation(ft.UserControl):
    def __init__(self, name: str = '', description: str = ''):
        super().__init__()
        self._name = name
        self._description = description

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
        return self._name

    @property
    def description(self) -> str:
        return textwrap.fill(self._description, self._description_wrap_length)

    def _on_change(self, e):
        if self._switch.value:
            self._on_select()
        else:
            self._on_deselect()
        self.update()

    def _on_select(self):
        print(f'Select: {self._name}')
        self._main_container.bgcolor = self._main_active_bgcolor
        self._switch_container.bgcolor = self._switch_active_bgcolor
        self._text_container.bgcolor = self._text_active_bgcolor

    def _on_deselect(self):
        print(f'Deselect: {self._name}')
        self._main_container.bgcolor = self._main_inactive_bgcolor
        self._switch_container.bgcolor = self._switch_inactive_bgcolor
        self._text_container.bgcolor = self._text_inactive_bgcolor

    def build(self):
        self._create_widgets()
        return self._get_layout()

    def _create_widgets(self):
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

        self._child_row = ft.Row()

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

        self._switch_container.content = self._switch
        self._text_container.content = self._text

        self._child_row.controls.append(self._switch_container)
        self._child_row.controls.append(self._text_container)

        self._main_container.content = self._child_row
        return self._main_container

    def select(self):
        self._switch.value = True
        self._on_select()
        self.update()

    def deselect(self):
        self._switch.value = False
        self._on_deselect()
        self.update()

    def set_data(self, name: str = '', description: str = '') -> None:
        self._name = name
        self._description = description
        self._set_data()
        self.set_border(None)

    def get_data(self) -> dict:
        return dict(
            name=self.name,
            description=self.description
        )

    def _set_data(self):
        self._switch.label = self.name
        self._text.value = self.description
        self._switch.update()
        self._text.update()

    def set_border(self, color=None):
        border = None
        if color:
            border = ft.border.all(5, color)
        self._main_container.border = border
        self._main_container.update()


class OperationList(ft.UserControl):
    """Inspired by: https://www.youtube.com/watch?v=hHs-IFcBTK8 """

    def __init__(self, title: str = 'Title'):
        super().__init__()
        self._title = title

        self._data = {}
        self._data_order = []
        self._list_column = ft.Column(scroll=ft.ScrollMode.ALWAYS)

        self._main_bgcolor = ft.colors.GREEN_50
        self._main_padding = 5
        self._main_border_radius = 10

    def build(self):
        self._create_widgets()
        return self._get_layout()

    def _create_widgets(self):
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

    def _set_list(self, sort_list):
        if sort_list:
            keys = sorted(self._data)
        else:
            keys = self._data.keys()
        for key in keys:
            print(key)
            oper = Operation(key, self._data[key])
            self._list_column.controls.append(
                ft.DragTarget(
                    group=self._title,
                    on_accept=self._on_drag_accept,
                    on_will_accept=self._on_will_accept,
                    on_leave=self._on_drag_leave,
                    content=ft.Draggable(
                        group=self._title,
                        content=oper,
                    )
                )
            )

    def _on_will_accept(self, e: ft.ControlEvent):
        """Called when drop is accepted"""
        print(f'{e.data=}')
        print(f'{e.control=}')
        print(f'{e.control.content=}')
        print(f'{e.control.content.content=}')
        # e.control.content.content.border = ft.border.all(
        #     5, ft.colors.RED if e.data == "true" else ft.colors.RED
        # )
        # e.control.content.content.update()
        e.control.content.content.set_border(ft.colors.RED)
        self._update_data_order(e.control.content, e.control)
        self._set_list_column()
        self.update()

    def _on_drag_leave(self, e: ft.ControlEvent):
        # e.control.content.content.border = None
        # e.control.content.content.update()
        e.control.content.content.set_border(None)

    def _on_drag_accept(self, e: ft.DragTargetAcceptEvent):
        src = self.page.get_control(e.src_id)
        # name = src.content.name
        # dest_index = self._list_column.controls.index(e.control)
        print('='*50)
        print(f'{src=}')
        # print(f'{name=}')
        print(f'{e.control=}')
        # print(f'{dest_index=}')
        # print()
        self._update_data_order(src, e.control)
        self._set_list_column()

        # source_index = None
        # for i, cont in enumerate(self._list_column.controls):
        #     if cont.content.content.name == name:
        #         source_index = i
        # print(f'{source_index=}')
        #
        # e.control.content.content.set_border(None)
        # cont = self._list_column.controls.pop(source_index)
        # self._list_column.controls.insert(dest_index, cont)
        self.update()

    def _get_index_from_draggable(self, draggable: ft.Draggable) -> int | None:
        for i, cont in enumerate(self._list_column.controls):
            if cont.content.content.name == draggable.content.name:
                return i

    def _get_index_from_drag_target(self, drag_target: ft.DragTarget) -> int | None:
        print(f'{drag_target=}')
        for i, cont in enumerate(self._list_column.controls):
            if cont.content.content.name == drag_target.content.content.name:
                return i

    def _update_data_order(self, src: ft.Draggable, dest: ft.DragTarget):
        src_index = self._get_index_from_draggable(src)
        dest_index = self._get_index_from_drag_target(dest)
        popped_item = self._data_order.pop(src_index)
        self._data_order.insert(dest_index, popped_item)
        print(self._data_order[:5])


    def old_update_data_order(self, src: ft.Draggable, dest_event: ft.DragTargetAcceptEvent):
        src_index = None
        for i, cont in enumerate(self._list_column.controls):
            if cont.content.content.name == src.content.name:
                src_index = i
        dest_index = self._list_column.controls.index(dest_event.control)

        src_data = src.content.get_data()
        dest_data = dest_event.control.content.content.get_data()

        popped_item = self._data_order.pop(src_index)
        self._data_order.insert(dest_index, popped_item)
        print(self._data_order[:5])

        # print()
        # print(f'{src_data=}')
        # print(f'{dest_data=}')

    def _set_list_column(self) -> None:
        """Sets the list column based on order in self._data_order"""
        for name, control in zip(self._data_order, self._list_column.controls):
            control.content.content.set_data(name=name, description=self._data[name])
        self.update()


    def set_list(self, data: dict, sort_list=False):
        self._data = data
        self._data_order = list(self._data.keys())
        self._reset_list()
        self._set_list(sort_list=sort_list)
        self.update()


class _OperationList(ft.UserControl):
    """Inspired by: https://www.youtube.com/watch?v=hHs-IFcBTK8 """

    def __init__(self, title: str = 'Title'):
        super().__init__()
        self._title = title

        self._data = {}
        self._data_order = []
        self._list_column = ft.Column(scroll=ft.ScrollMode.ALWAYS)

        self._main_bgcolor = ft.colors.GREEN_50
        self._main_padding = 5
        self._main_border_radius = 10

    def build(self):
        self._create_widgets()
        return self._get_layout()

    def _create_widgets(self):
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

    def _set_list(self, sort_list):
        if sort_list:
            keys = sorted(self._data)
        else:
            keys = self._data.keys()
        for key in keys:
            print(key)
            oper = Operation(key, self._data[key])
            self._list_column.controls.append(
                ft.DragTarget(
                    group=self._title,
                    on_accept=self._on_drag_accept,
                    on_will_accept=self._on_will_accept,
                    on_leave=self._on_drag_leave,
                    content=ft.Draggable(
                        group=self._title,
                        content=oper,
                    )
                )
            )

    def _on_will_accept(self, e: ft.ControlEvent):
        """Called when drop is accepted"""
        # print(f'{e.data=}')
        # print(f'{e.control=}')
        # print(f'{e.control.content=}')
        # print(f'{e.control.content.content=}')
        # e.control.content.content.border = ft.border.all(
        #     5, ft.colors.RED if e.data == "true" else ft.colors.RED
        # )
        # e.control.content.content.update()
        e.control.content.content.set_border(ft.colors.RED)

    def _on_drag_leave(self, e):
        # e.control.content.content.border = None
        # e.control.content.content.update()
        e.control.content.content.set_border(None)

    def _on_drag_accept(self, e: ft.DragTargetAcceptEvent):
        src = self.page.get_control(e.src_id)
        name = src.content.name
        dest_index = self._list_column.controls.index(e.control)
        print(f'{src=}')
        print(f'{name=}')
        print(f'{dest_index=}')

        source_index = None
        for i, cont in enumerate(self._list_column.controls):
            if cont.content.content.name == name:
                source_index = i
        print(f'{source_index=}')

        e.control.content.content.set_border(None)
        cont = self._list_column.controls.pop(source_index)
        self._list_column.controls.insert(dest_index, cont)
        self.update()

    def set_list(self, data: dict, sort_list=False):
        self._data = data
        self._data_order = list(self._data.keys())
        self._reset_list()
        self._set_list(sort_list=sort_list)
        self.update()


class old_OperationList(ft.UserControl):
    def __init__(self):
        super().__init__()

        self._data = {}
        self._widget_list = ft.Column()

        self._main_bgcolor = ft.colors.GREEN_50
        self._main_padding = 5
        self._main_border_radius = 10

    def build(self):
        self._create_widgets()
        return self._get_layout()

    def _create_widgets(self):
        self._main_container = ft.Container(
            bgcolor=self._main_bgcolor,
            padding=self._main_padding,
            border_radius=self._main_border_radius
        )

        self._list_column = ft.Column(
            scroll=ft.ScrollMode.ALWAYS,
        )

    def _get_layout(self):
        self.expand = True
        self._main_container.expand = True
        self._list_column.expand = True
        self._main_container.content = self._list_column
        return self._main_container

    def _reset_list(self):
        self._list_column.controls = []

    def _set_list(self, sort_list):
        if sort_list:
            keys = sorted(self._data)
        else:
            keys = self._data.keys()
        for key in keys:
            self._list_column.controls.append(
                Operation(key, self._data[key])
            )

    def set_list(self, data: dict, sort_list=False):
        self._data = data
        self._reset_list()
        self._set_list(sort_list=sort_list)
        self.update()



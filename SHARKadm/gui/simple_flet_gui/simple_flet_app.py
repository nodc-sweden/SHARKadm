# -*- coding: utf-8 -*-


import logging.handlers
import pathlib
import sys

# from SHARKadm.gui.tooltip_texts import TooltipTexts, get_tooltip_widget
# from SHARKadm.gui.texts import Texts
# from SHARKadm.gui.colors import Colors

import flet as ft
from flet_core.constrained_control import ConstrainedControl

from .widgets import operation

from SHARKadm import transformers

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
        self.file_picker = None
        self._config = {}
        self._attributes = {}
        self._progress_bars = {}
        self._progress_texts = {}
        self._instrument_items = {}
        self._current_source_instrument = None

        self._month_all_option = 'ALLA'

        self._toggle_buttons: list[ConstrainedControl] = []

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
        # self._initiate_pickers()
        self._build()
        # self._initiate_banner()

        # saves.add_control('_SHARKadm_file_path', self._SHARKadm_file_path)
        # saves.add_control('_template_directory', self._template_directory)
        # saves.add_control('_use_template_path', self._use_template_path)
        # saves.add_control('_ctd_directory', self._ctd_directory)
        # saves.add_control('_metadata_signature', self._metadata_signature)
        # saves.add_control('_metadata_project', self._metadata_project)
        # saves.add_control('_metadata_surface_layer', self._metadata_surface_layer)
        # saves.add_control('_metadata_bottom_layer', self._metadata_bottom_layer)
        # saves.add_control('_metadata_max_depth_diff_allowed', self._metadata_max_depth_diff_allowed)

        # saves.load(self)

        # self._on_blur_template_export_file_name()
        # self._on_blur_result_file_name()

        # self.use_template_path = self.use_template_path
        # self.template_directory = self.template_directory
        # self.create_result_path = self.create_result_path

    def update_page(self):
        self.page.update()

    def _build(self):
        row = ft.Row(expand=True)
        wid1 = operation.OperationList()
        wid2 = operation.old_OperationList()

        row.controls.append(wid1)
        row.controls.append(wid2)


        # wid1 = operation.Operation('TEST', 'Detta är ett test! Detta är ett test! Detta är ett test! Detta är ett '
        #                                   'test! Detta är ett test! Detta är ett test! Detta är ett test! Detta är '
        #                                   'ett test! Detta är ett test! Detta är ett test! Detta är ett test! Detta '
        #                                   'är ett test! Detta är ett test! Detta är ett test! Detta är ett test! '
        #                                   'Detta är ett test! Detta är ett test! Detta är dkljjlfdgjlj ldkjfg  '
        #                                   'lkjfgd . lsdkggdgh asg ett test! Detta är ett test! ')
        self.page.controls.append(row)
        self.update_page()
        data = transformers.get_transformers_description()
        wid1.set_list(data)
        wid2.set_list(data)

    def _create_widgets(self):
        """Entry point for creating widgets"""
        pass


def main(log_in_console):
    app = SimpleFletApp(log_in_console=log_in_console)
    return app
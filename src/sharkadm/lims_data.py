
import pathlib
import pandas as pd

from sharkadm import controller
from sharkadm import transformers
from sharkadm import exporters
from sharkadm import sharkadm_logger
from sharkadm.data import get_data_holder
from sharkadm import event


def get_row_data_from_lims_export(path: str | pathlib.Path, export_directory: str | pathlib.Path = None, export_log: bool = False) -> pd.DataFrame:
    path = pathlib.Path(path)
    data_holder = get_data_holder(path)
    c = controller.SHARKadmController()
    c.set_data_holder(data_holder)
    c.transform(transformers.AddRowNumber())
    c.transform(transformers.WideToLong())
    c.transform(transformers.MoveLessThanFlagRowFormat())
    c.transform(transformers.RemoveColumns('COPY_VARIABLE.*'))
    c.transform(transformers.MapperParameterColumn(import_column='SHARKarchive'))
    name = path.name.replace(' ', '_')
    if export_directory:
        export_directory = pathlib.Path(export_directory)
        if not export_directory.exists():
            raise NotADirectoryError(export_directory)
        c.export(exporters.TxtAsIs(export_directory=export_directory, export_file_name=f'{name}_row_format.txt',
                                   header_as='SHARKarchive', open_file_with_excel=False))

    if export_log:
        sharkadm_logger.create_xlsx_report(sharkadm_logger.adm_logger, export_directory=export_directory, open_report=True)

    return c.data
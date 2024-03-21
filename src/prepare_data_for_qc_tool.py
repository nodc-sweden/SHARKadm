import logging
import pathlib

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger()


from sharkadm import controller
from sharkadm import transformers
from sharkadm import exporters
from sharkadm import sharkadm_logger
from sharkadm.data import get_data_holder
from sharkadm import event

import warnings
import pandas as pd

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


def print_workflow(msg):
    print(msg)


event.subscribe('log_workflow', print_workflow)


if __name__ == "__main__":
    # source_directory = pathlib.Path(r"\\winfs-proj\data\proj\havgem\EXPRAPP\Exprap2023\Svea_v24_juni\LIMS\2023-06-18 2011-2023-LANDSKOD 77-FARTYGSKOD 10")
    # source_directory = pathlib.Path(r"C:\mw\data\qc_tools\lims\2024-01-17 1737-2024-LANDSKOD 77-FARTYGSKOD 10")
    source_directory = pathlib.Path(r"D:\Magnus\qc_tools\2024-03-14 1559-2023-LANDSKOD 77-FARTYGSKOD 10")
    export_directory = r'C:\mw\data\qc_tools/exports'

    sharkadm_logger.adm_logger.log_workflow('Starting controller script')

    data_holder = get_data_holder(source_directory)

    # data_holder = get_data_holder(sharkweb=True, data_type='PhysicalChemical', import_column='SHARKarchive', from_year=2021)

    if 1:
        c = controller.SHARKadmController()
        c.set_data_holder(data_holder)

        c.export(exporters.TxtAsIs(export_directory=export_directory, export_file_name='column_format.txt'))

        c.transform(transformers.WideToLong())
        c.transform(transformers.MoveLessThanFlagRowFormat())
        c.transform(transformers.RemoveColumns('COPY_VARIABLE.*'))
        c.transform(transformers.MapperParameterColumn(import_column='SHARKarchive'))
        name = source_directory.name.replace(' ', '_')
        # c.export(exporters.TxtAsIs(export_directory=export_directory, export_file_name=f'{name}_row_format_no_mapping.txt'))
        # c.export(exporters.TxtAsIs(export_directory=export_directory, header_as='SHARKarchive', open_file=True, open_directory=True))
        c.export(exporters.TxtAsIs(export_directory=export_directory, export_file_name=f'{name}_row_format.txt', header_as='SHARKarchive', open_file_with_excel=True))
        # c.export(exporters.TxtShortNames(export_directory=export_directory, export_file_name=f'{name}_row_format.txt'))

        print(f'{c.data=}')

    if 0:
        vals_before = []

        trans = [
            transformers.MoveLessThanFlag(),
            transformers.WideToLong(),
        ]

        vals_after = [
        ]

        exps = [
            # exporters.HtmlStationMap(open_map=True),
            exporters.TxtAsIs(export_directory=export_directory),
            # sharkdata_exporter,
            # exporters.StandardFormat(),
        ]

        c = controller.SHARKadmController()
        c.set_data_holder(data_holder)
        c.set_validators_before(*vals_before)
        c.set_transformers(*trans)
        c.set_validators_after(*vals_after)
        c.set_exporters(*exps)

        c.start_data_handling()

        print(f'{c.data=}')

        sharkadm_logger.create_xlsx_report(sharkadm_logger.adm_logger, open_report=True)



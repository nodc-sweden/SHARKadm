# -*- coding: utf-8 -*-

import logging
import pathlib

logger = logging.getLogger(__name__)


class SharkMetadata:
    # def __init__(self, path: str | pathlib.Path, encoding: str = 'cp1252') -> None:
    def __init__(self, data: dict) -> None:
        self._data = data
        self._path = data.pop("path", None)

    def __str__(self):
        lst = []
        for key, value in self._data.items():
            lst.append(f"{key}: {value}")
        return "\n".join(lst)

    def __getitem__(self, item: str) -> str:
        return self._data.get(item)

    @classmethod
    def from_txt_file(
        cls, path: str | pathlib.Path, encoding: str = "cp1252"
    ) -> "SharkMetadata":
        path = pathlib.Path(path)
        if path.suffix != ".txt":
            msg = f"File is not a valid shark_metadata text file: {path}"
            logger.error(msg)
            raise FileNotFoundError(msg)
        data = dict()
        data["path"] = path
        with open(path, encoding=encoding) as fid:
            for line in fid:
                if not line.strip():
                    continue
                if ":" not in line:
                    # Belongs to previous row
                    data[key] = f"{data[key]} {line.strip()}"
                    continue
                key, value = [item.strip() for item in line.split(":", 1)]
                key = key.lstrip("- ")
                data[key] = value
        return SharkMetadata(data)

    # @classmethod
    # def from_dv_template(cls, path: str | pathlib.Path):
    #     path = pathlib.Path(path)
    #     if path.suffix != '.xlsx':
    #         msg = f'Fil is not a valid xlsx dv template: {path}'
    #         logger.error(msg)
    #         raise FileNotFoundError(msg)
    #
    #     mapper = config.get_delivery_note_mapper()
    #
    #     dn = pd.read_excel(path, sheet_name='Förklaring')
    #     dn['key_row'] = dn[dn.columns[0]].apply(
    #         lambda x: True if type(x) == str and x.isupper() else False
    #     )
    #
    #     fdn = dn[dn['key_row']]
    #
    #     col_mapping = dict((c, col) for c, col in enumerate(dn.columns))
    #
    #     data = dict()
    #     data['path'] = path
    #     for key, value in zip(fdn[col_mapping[0]], fdn[col_mapping[2]]):
    #         if str(value) == 'nan':
    #             value = ''
    #         elif type(value) == datetime.datetime:
    #             value = value.date()
    #         data[mapper.get_txt_key_from_xlsx_key(key)] = str(value)
    #     data['data_format'] = data['format']
    #     data['import_matrix_key'] = data['format']
    #     if data['format'] == 'PP':
    #         data['data_format'] = 'Phytoplankton'
    #         data['datatyp'] = 'Phytoplankton'
    #         data['import_matrix_key'] = 'PP_REG'
    #
    #     return DeliveryNote(data)

    @property
    def data(self) -> dict[str, str]:
        return self._data

    @property
    def fields(self) -> list[str]:
        """Returns a list of all the fields in teh file. The list is unsorted."""
        return list(self._data)

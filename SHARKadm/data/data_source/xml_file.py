import logging
import xml.etree.ElementTree as ET

import pandas as pd

from .base import DataFile

logger = logging.getLogger(__name__)


class XmlDataFile(DataFile):

    @staticmethod
    def extract_data_from_element(element, data):
        for el in element:
            if not el.text.strip():
                XmlDataFile.extract_data_from_element(el, data)
            else:
                data[el.tag] = el.text

    def _load_file(self) -> None:
        tree = ET.parse(self._path)
        root = tree.getroot()

        rows = []
        und_data = {}
        for element in root.find('undersokning'):
            if element.tag == 'lokal':
                line_data = und_data.copy()
                XmlDataFile.extract_data_from_element(element, line_data)
                rows.append(line_data)
                s = pd.Series(line_data)
            else:
                und_data[element.tag] = element.text

        self._data = pd.DataFrame.from_dict(rows, orient='columns')



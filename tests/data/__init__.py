import pathlib
import pytest
import pandas as pd

DATA_DIRECTORY = pathlib.Path(__file__).parent


LIMS_DATA_PATH = DATA_DIRECTORY / r'2024-01-18 1030-2023-LANDSKOD 77-FARTYGSKOD 10\Raw_data\data.txt'
CHLOROPHYLL_COLUMN_DATA_PATH = DATA_DIRECTORY / r'SHARK_Chlorophyll_2021_SMHI_version_2023-04-27\processed_data\data.txt'


def get_lims_dataframe():
    return pd.read_csv(LIMS_DATA_PATH, encoding='cp1252', sep='\t', dtype=str)


def get_phytoplankton_dataframe():
    return pd.read_csv(CHLOROPHYLL_COLUMN_DATA_PATH, encoding='cp1252', sep='\t', dtype=str)
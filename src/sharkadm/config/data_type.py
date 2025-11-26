from sharkadm.config import import_matrix_paths
from sharkadm.config import get_import_matrix_config
from sharkadm.config.import_matrix import ImportMatrixMapper, ImportMatrixConfig


DTYPE_MAPPER = {
    "planktonbarcoding": "plankton_barcoding",
    "planktonimaging": "plankton_imaging",
    "epibenthosdropvideo": "epibenthos_dropvideo",
    "physicalandhemical": "physicalchemical",
}


def _get_mapped_datatype(data_type_synonym: str) -> str:
    dtype = (data_type_synonym.lower().replace("_", "")
             .replace(" ", ""))
    mapped = DTYPE_MAPPER.get(dtype)
    if mapped:
        return mapped
    return dtype


class DataType:

    def __init__(self, data_type_internal: str = None):
        self._data_type_internal = data_type_internal
        self._import_matrix_config: ImportMatrixConfig | None = None

    @property
    def data_type(self) -> str:
        return self._data_type_internal.capitalize()

    @property
    def data_type_internal(self) -> str:
        return self._data_type_internal

    @property
    def data_type_in_data(self) -> str:
        return self._data_type_internal.capitalize()

    @property
    def import_matrix(self) -> ImportMatrixConfig:
        if not self._import_matrix_config:
            self._import_matrix_config = get_import_matrix_config(self._data_type_internal)
        return self._import_matrix_config

    def get_mapper(self, matrix_key: str) -> ImportMatrixMapper | None:
        if not matrix_key:
            return
        if not self.import_matrix:
            return
        return self.import_matrix.get_mapper(matrix_key)


class DataTypeUnknown(DataType):

    @property
    def data_type(self) -> str:
        return "unknown"

    @property
    def data_type_internal(self) -> str:
        return "unknown"

    @property
    def data_type_in_data(self) -> str:
        return "unknown"


class DataTypePhysicalChemical(DataType):

    @property
    def data_type(self) -> str:
        return "PhysicalChemical"

    @property
    def data_type_internal(self) -> str:
        return self._data_type_internal

    @property
    def data_type_in_data(self) -> str:
        return "Physical and Chemical"


class DataTypeHandler:
    def __init__(self):
        self._data_types: dict[str, DataType] = {}
        self._load_data_types()

    def get_datatype(self, data_type_synonym: str) -> DataType:
        obj = self._data_types.get(data_type_synonym)
        if obj:
            return obj
        obj = _get_data_type(data_type_synonym)
        if obj:
            self._data_types[data_type_synonym] = obj
        return obj

    def _load_data_types(self):
        self._data_types = {}
        for internal, path in import_matrix_paths.items():
            self._data_types[internal] = DataType(internal)

        self._internal_dtypes = list()


CLASS_MAPPER = {
    "unknown": DataTypeUnknown,
    "physicalchemical": DataTypePhysicalChemical,
}


def _get_data_type(data_type_synonym: str) -> DataType | None:
    dtype = _get_mapped_datatype(data_type_synonym)
    if not import_matrix_paths.get(dtype):
        return
    cls = CLASS_MAPPER.get(dtype, DataType)
    return cls(dtype)


data_type_handler = DataTypeHandler()



if __name__ == "__main__":
    pass
from sharkadm.config import get_import_matrix_config, import_matrix_paths
from sharkadm.config.import_matrix import ImportMatrixConfig, ImportMatrixMapper

DTYPE_MAPPER = {
    "planktonbarcoding": "plankton_barcoding",
    "planktonimaging": "plankton_imaging",
    "epibenthosdropvideo": "epibenthos_dropvideo",
    "physicalandchemical": "physicalchemical",
}


def _get_mapped_datatype(data_type_synonym: str) -> str:
    dtype = data_type_synonym.lower().replace("_", "").replace(" ", "")
    return DTYPE_MAPPER.get(dtype, dtype)


class DataType:
    def __init__(self, data_type_internal: str | None= None):
        self._data_type_internal = data_type_internal
        self._import_matrix_config: ImportMatrixConfig | None = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}: {self._data_type_internal}"

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
            self._import_matrix_config = get_import_matrix_config(
                self._data_type_internal
            )
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

    @property
    def import_matrix(self) -> None:
        return None


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

    def get_data_type_obj(self, data_type_synonym: str) -> DataType:
        obj = self._data_types.get(data_type_synonym)
        print(f"{obj=}")
        if obj:
            return obj
        obj = _get_data_type(data_type_synonym)
        if obj:
            self._data_types[data_type_synonym] = obj
        return obj


CLASS_MAPPER = {
    "unknown": DataTypeUnknown,
    "physicalchemical": DataTypePhysicalChemical,
}


def _get_data_type(data_type_synonym: str) -> DataType | None:
    dtype_str = _get_mapped_datatype(data_type_synonym)
    if dtype_str == "unknown":
        return CLASS_MAPPER.get(dtype_str)(dtype_str)
    if not import_matrix_paths.get(dtype_str):
        return
    cls = CLASS_MAPPER.get(dtype_str, DataType)
    return cls(dtype_str)


data_type_handler = DataTypeHandler()


if __name__ == "__main__":
    dtype = data_type_handler.get_data_type_obj("profile")
    print(f"{dtype=}")

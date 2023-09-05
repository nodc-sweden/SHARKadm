

DATA_TYPE_MAPPING = {
    'epibenthosmartrans': 'epibenthos'
}


def get_data_type_mapping(data_type: str) -> str:
    return DATA_TYPE_MAPPING.get(data_type, data_type)
from sharkadm import transformers, controller
from . import data
from sharkadm.data import get_data_holder

c = controller.SHARKadmController()


def test_transformer_add_row_numer():

    holder = get_data_holder(data.LIMS_DATA_PATH)

    c.set_data_holder(holder)
    c.transform(transformers.AddRowNumber())

    col = 'row_number'
    assert col in c.data
    assert all(c.data[col])
    assert list(c.data[col]) == sorted(c.data[col], key=int)

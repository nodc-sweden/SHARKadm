import numpy as np


def decdeg_to_decmin(pos: float | str, nr_decimals: int = 2, with_space: bool = False):
    pos = float(pos)
    deg = np.floor(pos)
    minute = pos % deg * 60.0
    if nr_decimals:
        output = ('%%2.%sf' % nr_decimals % (deg * 100.0 + minute))
    else:
        output = str(deg * 100.0 + minute)
    if with_space:
        a, b = output.split('.')
        output = f'{a[:-2]} {a[-2:]}.{b}'
    return output


def decmin_to_decdeg(pos: str, nr_decimals: int = 2):
    pos = float(pos.replace(' ', ''))
    if pos >= 0:
        output = np.floor(pos / 100.) + (pos % 100) / 60.
    else:
        output = np.ceil(pos / 100.) - (-pos % 100) / 60.

    if nr_decimals:
        output = np.round(output, nr_decimals)

    return str(output)

import hashlib


def files_are_identical(file_path_1: str, file_path_2: str) -> bool:
    def _get_md5(path):
        with open(path, "rb") as fid:
            data = fid.read()
            return hashlib.md5(data).hexdigest()

    md5_1 = _get_md5(file_path_1)
    md5_2 = _get_md5(file_path_2)
    return md5_1 == md5_2


print(
    files_are_identical(
        r"C:\mw\Magnus_privat\arbetstid_2021.xlsx",
        r"C:\mw\Magnus_privat\arbetstid_2021 - kopia.xlsx",
    )
)


# with open(r"C:\Users\a001985\Downloads\windirstat1_1_2_setup.exe", 'rb') as fid:
#     data = fid.read()
#     calc_md5 = hashlib.md5(data).hexdigest()
#
# correct_md5 = '3abf1c149873e25d4e266225fbf37cbf'
#
# print(calc_md5)
# print(correct_md5)
# print(calc_md5 == correct_md5)

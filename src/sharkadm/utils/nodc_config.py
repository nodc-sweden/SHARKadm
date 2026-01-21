import pathlib

from sharkadm.utils.installations import verify_installation


def get_file_paths_to_svn_commit() -> list[pathlib.Path]:
    """Returns a list with paths that are linked to svn and that can be updated"""
    paths = []
    if verify_installation("nodc_georgaphy"):
        from nodc_geography import location_db

        paths.append(location_db.DB_PATH)
    return paths

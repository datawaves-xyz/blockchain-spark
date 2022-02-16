import os
from typing import AnyStr


def get_resource_path(groups: [str], file_name: str) -> str:
    current_file_dir = os.path.dirname(__file__)
    fixture_file_name = os.path.join(current_file_dir, *groups, file_name)

    if not os.path.exists(fixture_file_name):
        raise ValueError('File does not exist: ' + fixture_file_name)

    return fixture_file_name


def read_resource(groups: [str], file_name: str) -> AnyStr:
    fixture_file_name = get_resource_path(groups, file_name)

    with open(fixture_file_name, encoding="utf-8") as file_handle:
        return file_handle.read()

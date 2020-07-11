import importlib
import os
import sys

DEFAULT_APP_ATTRIBUTE = "app"


def load(app_path):
    _add_cwd_to_python_path()
    module_path, app_attribute = _parse_app_path(app_path)
    module = importlib.import_module(module_path)
    return getattr(module, app_attribute)


def _add_cwd_to_python_path():
    sys.path.append(os.getcwd())


def _parse_app_path(path):
    split_path = path.split(":")
    if len(split_path) == 1:
        return path, DEFAULT_APP_ATTRIBUTE
    if len(split_path) == 2:
        return split_path
    else:
        raise ValueError(f"App path is of the wrong format: {path}")

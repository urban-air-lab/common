import pytest
from ual.get_config import _get_caller_directory, get_config


def test_get_caller_directory_test_is_in_path():
    directory = _get_caller_directory(1)
    assert "test" in str(directory)


def test_get_config():
    config = get_config("ressources/test.yaml")
    assert config["name"] == "test"


def test_get_config_FileNotFoundError():
    with pytest.raises(FileNotFoundError):
        config = get_config("ressources/no_file.yaml")


def test_get_config_IOError():
    with pytest.raises(IOError):
        config = get_config("ressources/no_file.yaml")


def test_get_config_Exception():
    with pytest.raises(Exception):
        config = get_config(1)
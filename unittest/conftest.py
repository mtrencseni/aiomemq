import pytest

def pytest_addoption(parser):
    parser.addoption("--target", action="store", default="python")

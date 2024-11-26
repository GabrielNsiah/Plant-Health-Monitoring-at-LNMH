"""This file is for tests relating to the extract script."""
from extract import function_to_test
import pytest


def test_working_function():
    assert function_to_test() == True

"""This file is for tests relating to the extract script."""
from extract import function_to_test


def test_working_function():
    """A test function to allow GitHub Actions to complete"""
    assert function_to_test() is True

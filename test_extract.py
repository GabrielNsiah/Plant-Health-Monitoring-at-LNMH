"""This file is for tests relating to the extract script."""
from extract import function_to_test


def test_working_function():
    """Function to test the github Action"""
    assert function_to_test() is True

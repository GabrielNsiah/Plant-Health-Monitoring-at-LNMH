"""File for the streamlit dashboard"""

import streamlit as st
import pandas as pd
import pymssql
import os
from os import environ
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> None:
    """Returns a connection to the database"""
    connection = pymssql.connect(
        server=environ["DB_HOST"],
        port=environ["DB_PORT"],
        user=environ["DB_USER"],
        password=environ["DB_PASSWORD"],
        database=environ["DB_NAME"]
    )

    return connection


if __name__ == "__main__":
    pass

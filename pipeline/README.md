# ETL Pipeline

## Project Overview
This pipeline makes retrieves data via a request to the Liverpool Museum of Natural History's Plant API, transforms the data into a more readable `.csv` file, cleans the data of erroneous values and loads it into an Relational Database Service hosted on AWS.

Each step of the pipeline is available as a standalone python file:
- `extract.py`
- `transform.py`
- `load.py`

There is also a file to run the entire pipeline in sequence:
- `etl.py`
---

## Features
- Extract data from the Liverpool Museum of Natural History's Plant API
- Transform data into a `.csv` file.
- Load transformed data into an RDS hosted on Amazon Web Services.
- Logging is built in.

## Prerequisites

Before running the pipeline, ensure the following dependencies are installed:

- `python3.9` - For compatability with `pymmsql`
- `pytest` - For running tests
- `pylint` - For linting the code
- `requests` - For making HTTP requests
- `pandas` - For data manipulation
- `pytest-cov` - For measuring code coverage in tests
- `python-dotenv` - For loading environment variables from a `.env` file
- `pymssql` - For connecting to Microsoft SQL Server
- `boto3` - For interacting with AWS services
- `altair` - For creating interactive visualizations
- `streamlit` - For building and deploying web apps


## Environment Variables

To connect to the Microsoft SQL Server, you'll need to set up the following environment variables in your `.env` file:

- `DB_HOST` – The hostname or IP address of your SQL Server.
- `DB_PORT` – The port number for your SQL Server (default is typically `1433`).
- `DB_USER` – The username used to authenticate to your SQL Server.
- `DB_PASSWORD` – The password associated with the `DB_USER`.
- `DB_NAME` – The name of the database you want to connect to.
- `SCHEMA_NAME` – The name of the schema you want to work with in the database.

### Installation

To install the required dependencies, run:

```bash
pip install -r requirements.txt



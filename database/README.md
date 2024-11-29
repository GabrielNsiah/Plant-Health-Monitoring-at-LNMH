# Database

## Project Overview
The files in this folder are responsible for a multitude of things to do with the database:
- `schema.sql` - The schema for the database.
- `lambda_mover.py` - The code for moving old data from the database into the S3 bucket.
- `reset_db.py` - Code for resetting the database, removes all entries.

The database follows the below Entity-Relationship Diagram
![Entity Relationship Diagram](../architecture/ERD_diagram.png)
---

## Features
- Moves data older than 24 hours into an S3 bucket.
- Able to reset the state of the database

## Prerequisites

Before running files in the folder, ensure the following dependencies are installed:

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
Enter a virtual environment with:
```bash
python -m venv .venv
```
Enter the virtual environment with:
```bash
source .venv/bin/activate
```
To install the required dependencies, run:
```bash
pip install -r requirements.txt
```



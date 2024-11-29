# Streamlit

## Project Overview
The files in this folder are responsible for the creation of visualisations and the hosting of them on a streamlit dashboard.

Individually the files do as follows:
- `base_script.py` - The script that defines the base functions necessary for the other scripts.
- `continents.py` - The script that creates the graph for `Average Soil Moisture Per Continent Over Time`
- `combined_trends.py` - The script that creates the graph for `30-Minute Average Soil Moisture and Temperature over Time`
- `dashboard.py` - The code that hosts the streamlit dashboard.

To run the dashboard locally, in this directory, use the command:

```bash
streamlit run dashboard.py
```

The database follows the below wireframe.
![Dashboard Wireframe](../architecture/Dashboard-wireframe.png)

---


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
- `altair` - For creating the visualizations
- `streamlit` - For building and deploying the visualizations


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



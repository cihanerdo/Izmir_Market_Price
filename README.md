# IZMIR MARKET PRICE

![Untitled](README/Untitled.gif)

## Repository Cloning


To clone the repository, use the following command:


`git clone https://github.com/cihanerdo/Izmir_Market_Price.git`

## Database Connection

Enter the necessary database information in the .env file:

`DB_USERNAME=”your_username”`

`DB_PASSWORD=your_password`

`DB_HOST_IP=”your_ip”`

`DB_NAME=”your_database_name”`

## Installing Requirements

Install the required dependencies using the following command:

`pip install -r requirements.txt`

## How to Use

There are 2 different usage modes.

Mode used by giving date range:

uses the fetch_data_upload_postgre() function.
`python main.py -s 2023-01-01 -e 2023-02-01`

is used in the form.

![Untitled](README/Untitled.png)

The other mode is used for daily data extraction.

It uses the fetch_data_upload_postgre_today() function.
`python main.py`

is used in the form.

![Untitled](README/Untitled%201.png)

## Airflow Setup

Follow the steps below for Airflow installation:

`mkdir -p ./dags ./logs ./plugins ./config`

`echo -e "AIRFLOW_UID=$(id -u)" > .env`

`docker compose up —build -d`

Access the Airflow UI at [http://localhost:8080](http://localhost:8080/) after the installation is complete.


![Untitled](README/Untitled%202.png)

## **Project Example**

`python main.py -s 2024-01-01 -e 2024-01-05`

![Untitled](README/Untitled%203.png)

`python main.py`

![Untitled](README/Untitled%204.png)

## Database Example


![Untitled](README/Untitled%205.png)


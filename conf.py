import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


# Database Connection

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST_IP = os.getenv("DB_HOST_IP")
DB_NAME = os.getenv("DB_NAME")

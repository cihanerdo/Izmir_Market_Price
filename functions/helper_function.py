import pandas as pd
import json
from datetime import date, timedelta
import psycopg2
from conf import *
import requests
from functions.logger import logger
from sqlalchemy import create_engine



conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USERNAME,
    password=DB_PASSWORD,
    host=DB_HOST_IP
)

base_url = "https://openapi.izmir.bel.tr/api/ibb/halfiyatlari/sebzemeyve/"
def fetch_data(base_url, start_date, end_date):
    current_date = start_date
    result_data = []

    while current_date <= end_date:
        url = f"{base_url}{current_date.year}-{current_date.month}-{current_date.day}"
        response = requests.get(url)

        if response.status_code == 200:
            result_json = response.json()
            Avarage_Price = []
            Product_Name = []
            Unit = []
            Min_Price = []
            Max_Price = []
            Product_ID = []
            Product_Type_ID = []
            Prodcut_Type_Name = []

            for i in result_json["HalFiyatListesi"]:
                ortalama_ucret = i["OrtalamaUcret"]
                Avarage_Price.append(ortalama_ucret)

            for i in result_json["HalFiyatListesi"]:
                mal_adi = i["MalAdi"]
                Product_Name.append(mal_adi)

            for i in result_json["HalFiyatListesi"]:
                birim = i["Birim"]
                Unit.append(birim)

            for i in result_json["HalFiyatListesi"]:
                asgari_ucret = i["AsgariUcret"]
                Min_Price.append(asgari_ucret)

            for i in result_json["HalFiyatListesi"]:
                azami_ucret = i["AzamiUcret"]
                Max_Price.append(azami_ucret)

            for i in result_json["HalFiyatListesi"]:
                mal_id = i["MalId"]
                Product_ID.append(mal_id)

            for i in result_json["HalFiyatListesi"]:
                mal_tip_id = i["MalTipId"]
                Product_Type_ID.append(mal_tip_id)

            for i in result_json["HalFiyatListesi"]:
                mal_tip_adi = i["MalTipAdi"]
                Prodcut_Type_Name.append(mal_tip_adi)

            Izmir_Market_Dict = {"Avarage_Price": Avarage_Price, "Product_Name": Product_Name,
                                 "Unit": Unit, "Min_Price": Min_Price, "Max_Price": Max_Price,
                                 "Product_ID": Product_ID, "Product_Type_ID": Product_Type_ID,
                                 "Prodcut_Type_Name": Prodcut_Type_Name, "Date": current_date}

            df = pd.DataFrame(Izmir_Market_Dict)
            logger.debug(f"{current_date} Data Successfully Extracted.")
            result_data.append(df)
        else:
            if response.status_code == 204:
                logger.info(f"{current_date} No Data Found on the Date.")
            else:
                logger.error(f"Error fetching data for {current_date}. Status code: {response.status_code}")

        current_date += timedelta(days=1)

    return pd.concat(result_data, ignore_index=True)

def fetch_data_today(base_url, today):
    current_date = today
    result_data = []
    url = f"{base_url}{today}"
    response = requests.get(url)
    if response.status_code == 200:
        result_json = response.json()["HalFiyatListesi"]
        df = pd.json_normalize(result_json)
        df["Date"] = current_date
        logger.debug(f"{current_date}: Data Successfully Extracted.")
        result_data.append(df)
    else:
        if response.status_code == 204:
            logger.info(f"{current_date} No Data Found on the Date.")
        else:
            logger.error(f"Error fetching data for {current_date}. Status code: {response.status_code}")

    return pd.concat(result_data, ignore_index=True)

def create_postgresql_connection(DB_USERNAME, DB_PASSWORD, DB_HOST_IP, DB_NAME):
    con = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST_IP}:5432/{DB_NAME}"
    engine = create_engine(con)
    return engine
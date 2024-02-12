import pandas as pd 
from datetime import datetime, date, timedelta
import requests
import json
from functions.logger import logger 
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

def generate_url(date, **kwargs):

    url = f"https://openapi.izmir.bel.tr/api/ibb/halfiyatlari/sebzemeyve/{date}"
    logger.info(f"Url successfully generated. URL:{url}")
    return url

def fetch_data(**kwargs):
    ti = kwargs['ti']
    url = ti.xcom_pull(task_ids='generate_url_task')
    
    response = requests.get(url)
    if response.status_code == 200:
        result = response.json()
        logger.info("Data retrieval successful")
    else:
        raise AirflowFailException("Data not found. Failing the entire workflow.")

    return result

def json_to_dataframe(date, **kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='fetch_data_task')

    logger.debug("json_to_dataframe function started.")

    Avarage_Price = []
    Product_Name = []
    Unit = []
    Min_Price = []
    Max_Price = []
    Product_ID = []
    Product_Type_ID = []
    Prodcut_Type_Name = []


    for i in result["HalFiyatListesi"]:
        ortalama_ucret = i["OrtalamaUcret"]
        Avarage_Price.append(ortalama_ucret) 

    for i in result["HalFiyatListesi"]:
        mal_adi = i["MalAdi"]
        Product_Name.append(mal_adi) 
        
    for i in result["HalFiyatListesi"]:
        birim = i["Birim"]
        Unit.append(birim) 
        
    for i in result["HalFiyatListesi"]:
        asgari_ucret = i["AsgariUcret"]
        Min_Price.append(asgari_ucret) 
        
    for i in result["HalFiyatListesi"]:
        azami_ucret = i["AzamiUcret"]
        Max_Price.append(azami_ucret) 

    for i in result["HalFiyatListesi"]:
        mal_id = i["MalId"]
        Product_ID.append(mal_id) 

    for i in result["HalFiyatListesi"]:
        mal_tip_id = i["MalTipId"]
        Product_Type_ID.append(mal_tip_id)

    for i in result["HalFiyatListesi"]:
        mal_tip_adi = i["MalTipAdi"]
        Prodcut_Type_Name.append(mal_tip_adi) 


    Izmir_Market_Dict = {"Avarage_Price": Avarage_Price, "Product_Name": Product_Name, 
                        "Unit": Unit, "Min_Price": Min_Price, "Max_Price": Max_Price,
                        "Product_ID": Product_ID, "Product_Type_ID": Product_Type_ID, 
                        "Prodcut_Type_Name": Prodcut_Type_Name, "Date": date}

    df = pd.DataFrame(Izmir_Market_Dict)

    logger.info("Dataframe successfully generated.")

    return df

def dataframe_to_csv(csv_path, **kwargs):
    ti = kwargs['ti']
    DataFrame = ti.xcom_pull(task_ids='json_to_dataframe_task')

    DataFrame.to_csv(csv_path, index=False)
    logger.info("csv file created successfully.")
    
def upload_postgres(csv_file_path, **kwargs):
    DB_USERNAME = Variable.get("DB_USERNAME", default_var=None)
    DB_PASSWORD = Variable.get("DB_PASSWORD", default_var=None)
    DB_HOST_IP = Variable.get("DB_HOST_IP", default_var=None)
    DB_NAME = Variable.get("DB_NAME", default_var=None)
    engine = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST_IP}:5432/{DB_NAME}"
    df = pd.read_csv(csv_file_path)
    df.to_sql(name='Izmir_Market_Price', schema="stg" ,con=engine, if_exists='append', index=False)


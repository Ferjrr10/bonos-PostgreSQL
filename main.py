import pandas as pd
import openpyxl as op
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
import requests
from datetime import date
import os

today = date.today()
today_need = today.strftime("%d/%m/%Y")

# URLs for bond's prices
t = "20/05/2020"
urls = {
    "AL30D": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20902&fechaDesde={t}&fechaHasta={today_need}",
    "AL29D": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20900&fechaDesde={t}&fechaHasta={today_need}",
    "AL41D": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20907&fechaDesde={t}&fechaHasta={today_need}",
    "AE38D": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20903&fechaDesde={t}&fechaHasta={today_need}",
    "AL35D": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20905&fechaDesde={t}&fechaHasta={today_need}",
    "GD30D": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20916&fechaDesde={t}&fechaHasta={today_need}",
    "GD29D": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20914&fechaDesde={t}&fechaHasta={today_need}",
    "GD38D": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20932&fechaDesde={t}&fechaHasta={today_need}",
    "AL30": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20850&fechaDesde={t}&fechaHasta={today_need}",
    "AE38": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20853&fechaDesde={t}&fechaHasta={today_need}",
    "AL29": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20849&fechaDesde={t}&fechaHasta={today_need}",
    "AL41": f"https://www.cohen.com.ar/Financial/GetExcelReporteCotizacionesHistoricas?simbolo=20851&fechaDesde={t}&fechaHasta={today_need}"
}

# Function to download file using requests
def download_file(url, filename):
    response = requests.get(url, verify=False)  # SSL verification disabled
    response.raise_for_status()
    with open(filename, 'wb') as out_file:
        out_file.write(response.content)
    print(f"Downloaded {filename}")

# Download the files
for name, url in urls.items():
    download_file(url, f'./{name}.xlsx')

# Read data from Excel
dataframes = {}
for name in urls.keys():
    dataframes[name] = pd.read_excel(f'./{name}.xlsx')

# Function to connect to PostgreSQL
def connect_psql():
    try:
        host = "192.168.1.7"
        username = "root"
        password = "root"
        database = "postgres"
        conexion = create_engine(f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}")
        print('CONEXIÓN EXITOSA')
        return conexion
    except Exception as e:
        print(f"Error: {e}")
        return None

# Establish connection
con = connect_psql()

# Send DataFrame to PostgreSQL
if con is not None:
    tables = ["AL29D", "AL30D", "AL30", "AL41D", "AE38D", "AL29", "AL41", "AE38", "GD30D"]
    try:
        for table in tables:
            dataframes[table].to_sql(name=table, con=con, if_exists="append")
            print(f"DataFrame sent to table: {table}")

        # Remove the downloaded files
        for name in urls.keys():
            os.remove(f"./{name}.xlsx")

    except Exception as err:
        print(err)

# Query Database
try:
    query = f"SELECT * FROM AL29D;"
    df_database = pd.read_sql_query(query, con=con)
    print(df_database)
except Exception as err:
    print(err)

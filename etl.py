#Esse código precisa de um ambiente em máquina virtual para rodar.
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
from datetime import datetime, timedelta
import sqlite3

import os

CUR_DIR = os.getcwd()

def do_stg(df):

    month_dict = {
    'Jan': '01',
    'Fev': '02',
    'Mar': '03',
    'Abr': '04',
    'Mai': '05',
    'Jun': '06',
    'Jul': '07',
    'Ago': '08',
    'Set': '09',
    'Out': '10',
    'Nov': '11',
    'Dez': '12'
    }

    df = df[['COMBUSTÍVEL','ANO','REGIÃO','ESTADO','UNIDADE','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']]
    df = pd.melt(df, id_vars=['COMBUSTÍVEL','ANO','REGIÃO','ESTADO','UNIDADE'],value_vars=['Jan', 'Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'])
    df.variable = df.variable.map(month_dict)
    df['year_month'] =  df["variable"] + '/' + df["ANO"].astype(str)

    df.rename(columns={'COMBUSTÍVEL':'product','ESTADO': 'uf','value':'volume','UNIDADE':'unit'}, inplace=True)
    df.drop(columns=['REGIÃO','variable'],inplace=True)
    return df

def transform_data():
    df_derivative = pd.read_excel(f"{CUR_DIR}/raw_data/vendas-combustiveis-m3.xls", sheet_name="Derivative")
    df_diesel = pd.read_excel(f"{CUR_DIR}/raw_data/vendas-combustiveis-m3.xls", sheet_name="Diesel")
    
    stg_derivative = do_stg(df_derivative)
    stg_diesel = do_stg(df_diesel)

    stg_derivative.fillna(0.0,inplace=True)
    stg_diesel.fillna(0.0, inplace=True)

    stg_fuel = pd.concat([stg_derivative,stg_diesel])
    stg_fuel.to_csv("/tmp/processed_fuel.csv", index=False)

def load_data():
    conn = sqlite3.connect(f"{CUR_DIR}/airflow.db")
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS `venda_combustiveis` (
            `year_and_month` VARCHAR(45) NULL,
            `uf` VARCHAR(45) NULL,
            `product` VARCHAR(45) NULL,
            `unit` VARCHAR(45) NULL,
            `volume` DOUBLE NULL,
            `created_at` TIMESTAMP NULL,
            INDEX SHORT_DESC_IND(`created_at`));
    ''')
    records = pd.read_csv("/tmp/processed_fuel.csv")
    records.to_sql('venda_combustiveis', conn, if_exists='replace', index=False)

default_args = {
    'owner': 'RaizenInterview',
    'depends_on_past':False,
    'start_date':datetime(2022,2,19),
    'email':['otmpedrofaria@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    'retries':1
}

with DAG('raizen', schedule_interval=timedelta(days=1), 
            default_args=default_args, catchup=False) as dag:

            task_1 = PythonOperator(
                task_id = 'transform_data',
                python_callable = transform_data
            )

            task_2 = PythonOperator(
                task_id = 'load_data',
                python_callable = load_data
            )

task_1 >> task_2
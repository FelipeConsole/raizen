from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd

from datetime import datetime
import logging


path_xls = '/opt/airflow/data/vendas-combustiveis-2.xls'
path_csv = '/opt/airflow/data/tratado.csv'
sheets = ['DPCache_m3','DPCache_m3_2','DPCache_m3_3']

with DAG(
    dag_id='ETL',
    start_date=datetime(2023, 10, 9),
    schedule_interval = None
        ) as dag:    
    

    def read_and_merge(path_xls:str,sheets:str):
        """
            read excel file and concat the relevant sheets in one dataframe
            args:
                path, excel file path 
                sheets, list of sheets from the excel file to be concatenated
            return:
                
        """
        
        df_sheets = []
        for sheet in sheets:
            df_sheet = pd.read_excel(path_xls, sheet_name=sheet)
            df_sheets.append(df_sheet)

        #previously checked that the sheets has the same schema, so we can proceed with concat
        df_concat = pd.concat(df_sheets)
        
        logging.info(f'Colunas do df concatenado: {df_concat.columns}')
        logging.info(f'Shape do df concatenado: {df_concat.shape}')
        
        logging.info(f"Saving concatenated dataframe to csv file in: {path_csv}")
        df_concat.to_csv(path_csv,index=True)


    def transform(path_csv:str):
        """
            Read dataframe and group it
                args: path, The csv file path 
                return: None
        """

        logging.info("reading csv file")
        df = pd.read_csv(path_csv)

        logging.info("grouping dataframe")
        df_grouped = df.groupby(['ESTADO','COMBUSTÍVEL','ANO']).sum()
        
        logging.info(f'Shape of grouped dataframe: {df_grouped.shape}')

    def valida():
        logging.info("validando")
        

    read_and_merge_dfs = PythonOperator(
        task_id = 'read_excel',
        python_callable=read_and_merge,
        op_args=[path_xls,sheets]
        )

    transform_df = PythonOperator(
        task_id = 'transform',
        python_callable=transform,
        op_args=[path_csv]
        )
    
    validacao = PythonOperator(
        task_id = 'validacao',
        python_callable=valida,
        )
    
    
    read_and_merge_dfs >> transform_df >> validacao



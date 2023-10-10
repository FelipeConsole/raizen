from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

from datetime import datetime
import logging


path_in = '/opt/airflow/data/vendas-combustiveis-2.xls'
path_out = '/opt/airflow/data/tratado.csv'
# sheets = ['DPCache_m3','DPCache_m3_2','DPCache_m3_3']

with DAG(
    dag_id='etl_diesel',
    start_date=datetime(2023, 10, 9),
    schedule_interval = None
        ) as dag:    
    

    def read_and_merge(path_in:str,path_out:str):
        """
            read excel file and concat the relevant sheets in one dataframe
            args:
                path, excel file path 
                sheets, list of sheets from the excel file to be concatenated
            return:
                
        """
        

        df_sheet_m3_2 = pd.read_excel(path_in, sheet_name='DPCache_m3_2')
        
        logging.info(f'Colunas do df: {df_sheet_m3_2.columns}')
        logging.info(f'Shape do df: {df_sheet_m3_2.shape}')
        
        logging.info(f"Saving dataframe to csv file in: {path_out}")
        df_sheet_m3_2.to_csv(path_out,index=True)


    def transform(path_csv:str):
        """
            Read dataframe and group it
                args: path, The csv file path 
                return: None
        """

        logging.info("reading csv file")
        df = pd.read_csv(path_csv)

        logging.info('pivoting table')
        dfp = df.pivot_table(columns='ANO',aggfunc='sum')        
        
        logging.info(f'Shape of grouped dataframe: {dfp.shape}')

    def valida():
        logging.info("validando")
        

    read_and_merge_dfs = PythonOperator(
        task_id = 'read_excel',
        python_callable=read_and_merge,
        op_args=[path_in,path_out]
        )

    transform_df = PythonOperator(
        task_id = 'transform',
        python_callable=transform,
        op_args=[path_out]
        )
    
    validacao = PythonOperator(
        task_id = 'validacao',
        python_callable=valida,
        )
    
    
    read_and_merge_dfs >> transform_df >> validacao



from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import logging

import numpy as np
import pandas as pd

path_xlsx = '/opt/airflow/data/vendas-combustiveis-m3.xlsx'
path_out = '/opt/airflow/data/extracted_oil.csv'
# sheet_name = ['DPCache_m3','DPCache_m3_2','DPCache_m3_3']
sheet_name = 'Plan1'

with DAG(
    dag_id='etl_oil',
    start_date=datetime(2023, 10, 9),
    schedule_interval = None
        ) as dag:    
    

    def extract_tables(path_xlsx:str,sheet_name:str):
        """
            read excel file and extract data from pivot tables
            args:
                path, excel file path 
                sheet_name,  sheet from the excel file where the pivoted table is
            return:
                csv file with data
                
        """
        from openpyxl import load_workbook
        from openpyxl.pivot.fields import Missing

        workbook = load_workbook(path_xlsx)
        worksheet = workbook[sheet_name]
        pivot_name = 'Tabela dinâmica1'
        # Extract the pivot table object from the worksheet
        pivot_table = [p for p in worksheet._pivots if p.name == pivot_name][0]
        
        # Extract a dict of all cache fields and their respective values
        fields_map = {}
        for field in pivot_table.cache.cacheFields:
            if field.sharedItems.count > 0:
        # take care of cases where f.v returns an AttributeError because the cell is empty
        # fields_map[field.name] = [f.v for f in field.sharedItems._fields]
                l = []
                for f in field.sharedItems._fields:
                    try:
                        l += [f.v]
                    except AttributeError:
                        l += [""]
                fields_map[field.name] = l
        
        # Extract all rows from cache records. Each row is initially parsed as a dict
        column_names = [field.name for field in pivot_table.cache.cacheFields]

        rows = []
        for record in pivot_table.cache.records.r:
            # If some field in the record in missing, we replace it by NaN
            record_values = [
            field.v if not isinstance(field, Missing) else np.nan for field in record._fields
            ]

            row_dict = {k: v for k, v in zip(column_names, record_values)}

            #Shared fields are mapped as an Index, so we replace the field index by its value
            for key in fields_map:
                row_dict[key] = fields_map[key][row_dict[key]]

            rows.append(row_dict)
  
        df = pd.DataFrame.from_dict(rows)
        print(df)


    def transform(path_csv:str):
        """
            Read dataframe and group it
                args: path, The csv file path 
                return: None
        """
        logging.info("Transforming")
  
    def valida():
        logging.info("Validando")
        

    extract_pivot_table = PythonOperator(
        task_id = 'extract_pivot_tables',
        python_callable=extract_tables,
        op_args=[path_xlsx,sheet_name]
        )

    # transform_df = PythonOperator(
    #     task_id = 'transform',
    #     python_callable=transform,
    #     op_args=[path_out]
    #     )
    
    # validacao = PythonOperator(
    #     task_id = 'validacao',
    #     python_callable=valida,
    #     )
    
    
    extract_pivot_table


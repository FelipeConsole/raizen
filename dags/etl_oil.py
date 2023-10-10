from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import logging

import numpy as np
import pandas as pd

path_xlsx = '/opt/airflow/data/vendas-combustiveis-m3.xlsx'
path_out = '/opt/airflow/data/extracted_oil.csv'
path_parquet = '/opt/airflow/data/oil_parquet'
sheet_name = 'Plan1'
cols_to_drop = ['TOTAL','REGIÃO']
dict_m = {"Jan": 1, "Fev": 2, "Mar": 3, "Abr": 4, "Mai": 5,"Jun": 6, "Jul": 7, "Ago": 8, "Set": 9, "Out": 10, "Nov": 11, "Dez": 12}


with DAG(
    dag_id='etl_oil',
    start_date=datetime(2023, 10, 9),
    schedule_interval = None
        ) as dag:    
    

    def extract_tables(path_xlsx:str,path_out:str,sheet_name:str):
        """
            read excel file, extract data from pivot table and save the data in a csv file.
            args:
                path_xlsx, excel file path;
                path_out, where extracted data path will be saved; 
                sheet_name,  name of sheet from the excel file where the pivoted table is.
            return: None
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

        df.to_csv(path_out)

    
    def transform(path_csv:str, path_parquet:str,cols_to_drop:list, dict_m:dict):
        """
            Read csv file from extracted the pivoted tables,
            melt the months columns and return the dataframe with the appropriate schema.
            The transformed dataframe is saved as a parquet file
            args:
                path_csv: path to the csv file
                path_parquet: path to save parquet file
                cols_to_drop: columns to be dropped from csv file 
                dict_m: a dictionary to map the months name to the month number  
            return: None
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        logging.info("Transforming")

        df = pd.read_csv(path_csv)
        logging.info(f'Columns of df: {df.columns}')
        logging.info(f'Shape of df: {df.shape}')

        df.drop(columns=cols_to_drop, inplace=True)

        #melt month columns
        df = pd.melt(df, id_vars = ["COMBUSTÍVEL", "ANO", "ESTADO"], value_vars = dict_m.keys(), var_name = "month", value_name = "volume")

        #create constant columns
        df['unit'] = 'm3'
        df['created_at'] = datetime.now()

        # rename columns accordingly
        df.rename(columns={'COMBUSTÍVEL': 'product','ESTADO': 'uf'},inplace=True)

        #create year_month column
        df['year_month'] = df['ANO'].astype(int).astype(str) + '-' + df['month'].replace(dict_m).astype(str)
        df['year_month'] = pd.to_datetime(df['year_month'], format='%Y-%m')

        #reordering columns accordingly
        columns_order = ['year_month','uf','product','unit','volume','created_at']
        df = df[columns_order]

        logging.info(f"Columns of df: {df.columns}")
        logging.info(f"Shape of df: {df.shape}")
        logging.info(f"Data types: {df.dtypes}")
        
        # Setting right data types
        df['uf'] = df['uf'].astype('string')
        df['product'] = df['product'].astype('string')
        df['unit'] = df['unit'].astype('string')
        df['volume'] = pd.to_numeric(df['volume'])

        logging.info(f"Final data types: {df.dtypes}")

        logging.info(f'writing df as parquet file at {path_parquet}')
        tab = pa.Table.from_pandas(df)
        pq.write_table(tab, path_parquet, compression='SNAPPY')


        # By experimenting, we found that --in this case-- saving as parquet with partitioned columns is not the best choice
        # in terms of size of the final file.
        # output_dir = '../data/partitioned_parquet_data'
        # partition_cols = ['uf'];  25 partitions OR
        # partition_cols = ['year_month'];  252 partitions 
        # df.to_parquet(output_dir, partition_cols=partition_cols, engine='pyarrow')

    def valida():
        """
            Validação do output
        """
        logging.info("Validando")
        
        def check_sum():
            pass
        

    extract_pivot_table = PythonOperator(
        task_id = 'extract_pivot_tables',
        python_callable=extract_tables,
        op_args=[path_xlsx,path_out,sheet_name]
        )

    transform_df = PythonOperator(
        task_id = 'transform',
        python_callable=transform,
        op_args=[path_out,path_parquet,cols_to_drop,dict_m]
        )
    
    # validacao = PythonOperator(
    #     task_id = 'validacao',
    #     python_callable=valida,
    #     )
    
    
    extract_pivot_table >> transform_df



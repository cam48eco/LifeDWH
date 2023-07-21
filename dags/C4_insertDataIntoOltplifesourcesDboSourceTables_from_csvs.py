# Import libraries 

import pandas as pd
import pyodbc 
import datetime as dt

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


# Setting arguments for DAG

default_args = {
    'owner': 'pawel',
    'start_date': dt.datetime(2023, 7, 20),
    'retries': 1, #the number of retries that should be performed before failing the task
    'retry_delay': dt.timedelta(minutes=1), # delay between retries
}

# Passing arguments to DAG, set run interval, define tasks with operators (Bash/Python) and relations among them    

with DAG('C4_insertDataIntoOltplifesourcesDboSourceTables_from_csvs',
         default_args=default_args,
         schedule_interval= '7 6 * * *'
         ) as dag:

# Tasks definitions 

    # Communicate process start 
    print_insertintosourcetables = BashOperator(task_id='Print_info',
                               bash_command='echo "I am inserting / updating data into source tables in oltplife from csvs: observations "')

    # Execute sql code from file 
    exe_createandfeedtablesfromcsv = MsSqlOperator(
       task_id="create_table_from_external_file",
       mssql_conn_id="airflow_mssql_f",
       # T-SQL file - path relative to DAG
       sql="C4_insertDataIntoOltplifesourcesDboSourceTables_from_csvs.sql",
       dag=dag,
     )

    # Communicate process completions 
    print_result = BashOperator(task_id='Communicate_result',
                               bash_command='echo "Task executed"')


# Dependencies - Sequence

print_insertintosourcetables >> exe_createandfeedtablesfromcsv >> print_result



 

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
    'owner': 'life',
    'start_date': dt.datetime(2023, 7, 7),
    'retries': 1, #the number of retries that should be performed before failing the task
    'retry_delay': dt.timedelta(minutes=1), # delay between retries
}


# Passing arguments to DAG, set run interval, define tasks with operators (Bash/Python) and relations among them    

with DAG('C2_insertSourceTablesIntoStagingTables',
         default_args=default_args,
         schedule_interval= '3 6 * * *'
         ) as dag:


# Tasks definitions 

    # Communicate process start 
    print_start = BashOperator(task_id='Print_info',
                               bash_command='echo "I am inserting / updating data from tables in oltplifesource into oltplifestaging "')

    # Execute sql code from file 
    exe_feedstagingtablesfromsource = MsSqlOperator(
       task_id="feed_staging_tables_from_source",
       mssql_conn_id="airflow_mssql_f",
       # T-SQL file - path relative to DAG
       sql="C2_insertSourceTablesIntoStagingTables.sql",
       dag=dag,
     )

    # Communicate process completions 
    print_result = BashOperator(task_id='Communicate_result',
                               bash_command='echo "Task executed"')



# Dependencies - Sequence

print_start >> exe_feedstagingtablesfromsource >> print_result



 

# Import libraries 

import pandas as pd
import datetime as dt
import gdown

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


# Method to be callable to retrieve data. Preffered PythonOperator. 

def retrieveobservationdatafromgoogle ():
    import gdown
    # Url for google drive directory with 33 files with observations. Written into /home/pawelc/observations.
    url = "https://drive.google.com/drive/folders/1vit8l2RQdz1xgrHEYeqUuz4MB73s53G5"
    gdown.download_folder(url, quiet=True, use_cookies=False)

def retrievecommunitiesdatafromgoogle ():
    import gdown
    # Url for google drive directory with 1 file with communities details. Written into /home/pawelc/communities.
    url = "https://drive.google.com/drive/folders/1zJvBEfYQirQH3WtjSt_KS_GfFxMSXXqo"
    gdown.download_folder(url, quiet=True, use_cookies=False)

# Arguments for DAG

default_args = {
    'owner': 'pawel',
    'start_date': dt.datetime(2023, 7, 20),
    'retries': 1, #the number of retries that should be performed before failing the task
    'retry_delay': dt.timedelta(minutes=1), # delay between retries
}

# Passing arguments to DAG, set run interval, define tasks with operators (Bash/Python) and relations among them    

with DAG('C1_retrieveFolderPreSourceDataFromGoogleSheets',
         default_args=default_args,
         schedule_interval= '1 6 * * *'
         ) as dag:


# Tasks definitions 


    # Communicate process start 
    print_retrievefromgoogle = BashOperator(task_id='Print_info',
                               bash_command='echo "I am retrieving observation and comminities data from google into local destination as csvs"')

    # Execute code  
    exe_retrieveobservationsfromgoogle = PythonOperator(
       task_id="retrieveobservationsfromgoogle",
       python_callable=retrieveobservationdatafromgoogle)

   # Execute code  
    exe_retrievecommunitiesfromgoogle = PythonOperator(
       task_id="retrievecommunitiesfromgoogle",
       python_callable=retrievecommunitiesdatafromgoogle)

    # Communicate process completions 
    print_result = BashOperator(task_id='Communicate_result',
                               bash_command='echo "Task executed"')


# Dependencies - Sequence

print_retrievefromgoogle >> exe_retrieveobservationsfromgoogle >> exe_retrievecommunitiesfromgoogle >> print_result

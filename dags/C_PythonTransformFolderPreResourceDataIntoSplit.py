# Import libraries 

import pandas as pd
import datetime as dt
import gdown

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


# Method to be callable to retrieve data. Preffered PythonOperator. 

def splitingfile():

import pandas as pd
import os 

# Create list of csv files with data indicating directory where the files are stored  

datafilelist = os.listdir('D:\Path\To\csv\files')

# Create intial dataframe (from the first csv dataset) with columns: 'community names' and 'dates of observation'

dftemp = pd.read_csv('D:\Path\To\csv\files\{}'.format(datafilelist[1]), sep = ";")  
dftemp =  dftemp[['gmina_name', 'date']]

# Splitting data from every .csv file from columns to separate csv files - one for each variable  
# In result 33 .csv files will be transformed into 126 separate .csv files (each file for separate variable, the filename named after variable, unified structure). 

# Iterate over every file in the directory with .csv files 
for element in datafilelist:
    if 'csv' in element:
        # Read .csv as dataframe 
        datafile = pd.read_csv('D:\Path\To\csv\files\{}'.format(element), sep = ";")  
        # Get number of columns in the csv file 
        nbcolums = datafile.shape[1]
        # First two columns ("0" and "1") are:  'community names' ("0") and 'years of observation' ("1")
        # Variables values are stored in column(s) "2" and next.   
        # Therefore iterate over colums with variables values (with number "2" or over) 
        for i in range(2, nbcolums):
            # Concatenate dftemp with every column with variables values from csv file 
            newfiletemp = pd.concat([dftemp, datafile.iloc[:, [i]]], axis = 1)
            # Rename name of column with values into 'value' 
            newfiletemp.columns = newfiletemp.columns.str.replace(str(newfiletemp.columns[2]), 'value')
            # Write the dataframe into separate file naming file according to variable name
            newfiletemp.to_csv('D:\Path\To\csv\files\AfterSplit\{}.csv'.format(datafile.columns[i]), sep = ";", index=False) 



# Arguments for DAG

default_args = {
    'owner': 'life',
    'start_date': dt.datetime(2023, 7, 19),
    'retries': 1, #the number of retries that should be performed before failing the task
    'retry_delay': dt.timedelta(minutes=1), # delay between retries
}

# Passing arguments to DAG, set run interval, define tasks with operators (Bash/Python) and relations among them    

with DAG('C_PythonTransformFoldderPreResourceDataIntoSplit',
         default_args=default_args,
         schedule_interval= '0 6 * * *'
         ) as dag:


# Tasks definitions 


    # Communicate process start 
    print_observationssplit = BashOperator(task_id='Print_info',
                               bash_command='echo "I am splitting observation data into separate csv files for each variable"')

    # Execute code  
    exe_observationssplit = PythonOperator(
       task_id="splitobservations",
       python_callable=splitingfile)

    # Communicate process completions 
    print_result = BashOperator(task_id='Communicate_result',
                               bash_command='echo "Task executed"')


# Dependencies - Sequence

print_observationssplit >> exe_rexe_observationssplit  >> print_result

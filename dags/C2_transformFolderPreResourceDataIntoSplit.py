
import os
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# DAG definition - DAG with branching task

default_args = {
    'owner': 'life',
    'retries': 1, #the number of retries that should be performed before failing the task
    'retry_delay': dt.timedelta(minutes=1), # delay between retries
}


dag = DAG(
    dag_id="C2_transformFolderPreResourceDataIntoSplit",
    schedule_interval= '5 6 * * *', # 4 minutes after retrieval from Google Drive
    start_date=dt.datetime(2023, 7, 20),
    default_args=default_args,
)



# Branching task

@task.branch(task_id="branching")
def do_branching():

	datafilelist = os.listdir('/home/path/observations')
	if len(datafilelist)==0:
		return "result_empty"
	if len(datafilelist)!=0:
		return "result_non_empty"

branching = do_branching()

# Following tasks -  branch with non-empty observations directory

result_non_empty = EmptyOperator(task_id="result_non_empty", dag=dag)

def splitingfile():

	import pandas as pd
	import os 

	# Create list of csv files with data indicating directory where the files are stored  

	datafilelist = os.listdir('/home/path/observations')
	# Create intial dataframe (from the first csv dataset) with columns: 'community names' and 'dates of observation' when 'observation' folder not empty 
	if len(datafilelist)!=0:
		dftemp = pd.read_csv('/home/path/observations/{}'.format(datafilelist[1]), sep = ";")  
		dftemp =  dftemp[['gmina_name', 'date']]

		# Splitting data from every .csv file from columns to separate csv files - one for each variable  
		# In result 33 .csv files will be transformed into 126 separate .csv files in 'observationsaftersplit' directory (each file for separate variable, the filename named after variable, unified structure). 

		# Iterate over every file in the directory with .csv files 
		for element in datafilelist:
			if 'csv' in element:
				# Read .csv as dataframe
				datafile = pd.read_csv('/home/path/observations/{}'.format(element), sep = ";")
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
					newfiletemp.to_csv('/home/path/observationsaftersplit/{}.csv'.format(datafile.columns[i]), sep = ";", index=False) 


splitingfile =  PythonOperator(
       task_id="splitingfile", 
       python_callable=splitingfile)


# Following tasks -  branch with empty 'observations' directory

result_empty = BashOperator(task_id='result_empty',
                               bash_command='echo "Task executed - folder empty, nothing to split"')



# Dependencies - Sequence

branching >> result_non_empty >> splitingfile  
branching >> result_empty  
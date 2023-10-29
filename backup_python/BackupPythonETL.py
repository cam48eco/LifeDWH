# Backup script to enable when airflow is not connected

# Import data from google drive (C1)
# Please open terminal, open venv, open directory and execute python
# Than, execute line by line using F1


import pandas as pd
import datetime as dt
import gdown

def retrieveobservationdatafromgoogle ():
    import gdown
    # Url for google drive directory with 33 files with observations. Written into /home/pawelc/observations.
    url = "https://drive.google.com/drive/folders/1vit8l2RQdz1xgrHEYeqUuz4MB73s53G5"
    gdown.download_folder(url, quiet=True, use_cookies=False)

retrieveobservationdatafromgoogle()
# print(os.getcwd())

# Move manually to C:\observations

# Path with observations after download: C:\Users\dell\Desktop\DWHonPremise\observations

# Exit python and go again

# ONLY IF CHANGE IN COMMUNITIES !!

import pandas as pd
import datetime as dt
import gdown

def retrievecommunitiesdatafromgoogle ():
    import gdown
    # Url for google drive directory with 1 file with communities details. Written into /home/pawelc/communities.
    url = "https://drive.google.com/drive/folders/1zJvBEfYQirQH3WtjSt_KS_GfFxMSXXqo"
    gdown.download_folder(url, quiet=True, use_cookies=False)

retrievecommunitiesdatafromgoogle()

# Move manually to C:\communities

# Transform data from csv (C2) transformFolderPreResourceDataIntoSplit.py

def splitingfile():
	import pandas as pd
	import os 
	# Create list of csv files with data indicating directory where the files are stored  
	datafilelist = os.listdir('C:/observations')
	# Create intial dataframe (from the first csv dataset) with columns: 'community names' and 'dates of observation' when 'observation' folder not empty 
	if len(datafilelist)!=0:
		dftemp = pd.read_csv('C:/observations/{}'.format(datafilelist[1]), sep = ";")  
		dftemp =  dftemp[['gmina_name', 'date']]
		# Splitting data from every .csv file from columns to separate csv files - one for each variable  
		# In result 33 .csv files will be transformed into 126 separate .csv files in 'observationsaftersplit' directory (each file for separate variable, the filename named after variable, unified structure). 
		# # Iterate over every file in the directory with .csv files 
		for element in datafilelist:
			if 'csv' in element:
				# Read .csv as dataframe
				datafile = pd.read_csv('C:/observations/{}'.format(element), sep = ";")
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
					newfiletemp.to_csv('C:/observationsaftersplit/{}.csv'.format(datafile.columns[i]), sep = ";", index=False) 


splitingfile()




# NOW EXECUTE SQL QUERIES 


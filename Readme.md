![LifeLogo](https://github.com/cam48eco/LifeDWH/blob/main/img/LIFE.png)

# ETL solution for Life

## Motivation 
The motivation to prepare the solution was a need to facilitate the data flow and ensuring access to data neccessary for analyses of socio-economic aspects of Pilica catchment area, consisting out of 93 communities  (gmina) on NUTS5 level.


## Scope
The repository documents the steps and include resources used for solution creation and set up of data warehouse (under SQL Server 2017) and its maitenance with orchestration tool (Airflow).
The following issues are covered:
- I. Data sources identification, preprocessing and storage 
- II. ETL and Data warehouse design and implementation
- III. Automation of the ETL process with Airflow.


## I. Data sources identification, initial preprocessing and storage 

According to project assumptions, the main data source was a Central Statistical Office of Republic of Poland - Local Data Bank (https://stat.gov.pl/en/), supplemented by other sources, such as data from:
- interviews conducted during the project, 
- ministries and governmental agencies (data from websites or retrieved on formal request),
- desk research results (data from the documents). 

In all of above mentioned cases, it was neccessary to transform the data from the original formats into one format common for all (.csv) with common data structure.  
In result, each .csv file included following columns:
- column "gmina_name" with the name of the local community,
- column "date" with the indication of the date, when observation has been made (01.01.1995 to 01.01.2022 with one-year step),
- column(s) named after variable(s) name with the values registered for variable for given "gmina_name" on given "date".   

In result, 33 .csv files have been created and filled with data (depending on the data source, this process was more or less automated). The source' data catalogue includes: 
- 1 file with data derived from interviews,
- 3 files with data derived from desk research (documents)
- 1 file with data derived from ministries / agencies, 
- 28 files with data derived from Central Statistical Office.

All above mentioned .csv files are stored in this repository (here) and are reviewed and updated periodically (at least once per month) manually. In case the new observations are detected by research team, the relevant .csv file is updated. In case if the new data source with new data will appear, the new .csv file will be created.

An additional data source is a .csv file with data describing features of each out of 93 communities (s_multi_dimension_gmina.csv; access [here](https://github.com/user/repo/blob/branch/other_file.md)). 

## II. ETL and Data warehouse design and implementation


### 2.1. Preparatory tasks  

#### 2.1.1. Data transformation 

For the further use of data included in the 33 .csv files (excluding s_multi_dimension_gmina.csv) for data warehouse purposes, it was neccessary to conduct an additional transformation to receive a separate tables in separate .csv files for each of the variables, adding the variable name as a name of newly created .csv file. To avoid time-consumig work during this job, the simple script in python (see below) has been used, to automatise the process of creation of separate file for each variable. 

```python 
import pandas as pd
import os 

# Create list of csv files with data indicating directory where the files are stored  

datafilelist = os.listdir('D:\Path\To\csv\files')

# Create intial dataframe (from the first csv dataset) with columns: 'community names' and 'dates of observation'

dftemp = pd.read_csv('D:\Path\To\csv\files\{}'.format(datafilelist[1]), sep = ";")  
dftemp =  dftemp[['gmina_name', 'date']]

# Splitting data from every .csv file from columns to separate csv files - one for each variable  
# In result 33 .csv files will be transformed into 126 separate .csv files (each file for separate variable, the filename named after variable, unified structure). 


def splitingfile():
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
                newfiletemp.to_csv('D:\Path\To\csv\files\AfterSplit\{}.csv'.format(datafile.columns[i]), index=False) 

splitingfile()

```
In result 33 .csv files with data were transformed into 126 separate .csv files (each file for separate variable, the filename named after variable, unified structure: three columns: "gmina_name", "date" and "value"), and stored in separated part of this repository [here](https://github.com/user/repo/blob/branch/other_file.md). 

 
#### 2.1.2. Creation and initial feed of OLTP database for storing 'transactional' data 

To ensure the smooth functioning of data warehouse, it was neccessary to design and set-up a database, where 'transactional' data, being stored so far in flat files (.csv; see previous chapters) will be stored. 
The term 'transactional' relates in this specific case, to the data with observations with values of various variables in various dates. The database (updated periodically throghout Airflow taks, what will be discussed further), will serve as a 'point of departure' for ETL process for the Data Warehouse. 
In our case, the database is very simple and consist out of tables with data with variables values observations (126 tables) and one table with specific information on communities - in both cases filled periodically from flat .csv files. 

For this purpose the following steps in the SQL Server environment (SSMS) have been conducted.

**Creation of the database**

![OltpLogo](https://github.com/cam48eco/LifeDWH/blob/main/img/CreateOLTP.png)


**Creation of the database tables and fetching with data** 

Initial creation of the database tables for storing observations data (126 tables) and information on communities (1 table) along with data import from the csv's is conducted with Airflow task, using DAG with mssql operator, allowing to implement queries on oltplife database. The core of the approach is the SQL server script, focused not only on the initial tables creation and fetching, but enabling their updating without duplicates in given intervals (here: every 30 days), as well. 
"Import flat files" option present in SSMS was not applicable as it serves only single files. SSMS Express version, used for this solution, does not include task scheduling feature, so preparation of dedicated SQL code was neccessary. 


























 






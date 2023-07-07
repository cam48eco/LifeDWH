![LifeLogo](https://github.com/cam48eco/LifeDWH/blob/main/img/LIFE.png)

# ETL solution for Life

## Motivation 
The motivation to prepare the solution was a need to facilitate the data flow and ensuring access to data neccessary for analyses of socio-economic aspects of Pilica catchment area, consisting out of 93 communities  (gmina) on NUTS5 level.
Previously, the dedicated tool [(R package)](https://github.com/ap48atgazetadotpl/R_GUS_retrieving) for automated data retrieval from Central Statistical Office (GUS) in Poland, based on GUS API, has been elaborated, however the work has been suspended because of integration limitations. 

## Assumptions
The point of departure for solution preparation was a statement, that the solution should be deployed in a way, assuming usage of on-premise infrastructure, including an server working on local machine (e.g. laptop) connected to web, due to lack of financial resources for estabilishment and maitenance of external server or cloud solutions. In addition, the free versions of software (SSMS Express) have to be used, limiting the access to useful features, what resulted in need to prepare own scripts instead of automate some of the tasks.
The solution' architecture follows these limitations, however the implemented approach (T-SQL scripts, Airflow DAGs, etc.) can easily be adopted, after minor changes, to prepare a similar solutions in on-premise servers or (with more effort) in the cloud. Disrgegard on above mentioned limits, some of cloud solutions have been also discussed and implemented (in particular for visualization), to take the adventages from such kind of technologies as Tableau and Looker. 

## Scope
The repository documents the steps and include resources used for solution creation and set up of data warehouse (under SQL Server 2017) and its maitenance with orchestration tool (Airflow).
The following issues are covered:
- I. Data sources identification, preprocessing and storage 
- II. ETL and Data warehouse design and implementation
- III. Automation of the ETL process with Airflow.

Fig. 1. Brief presentation of the pipeline / ETL architecture assumptions
![LifeBPM](https://github.com/cam48eco/LifeDWH/blob/main/img/diagram_Airflow.svg)

Source: Author own elaboration 


## I. Data sources identification, initial preprocessing and storage 

According to project assumptions, the main data source was a Central Statistical Office of Republic of Poland - Local Data Bank (https://stat.gov.pl/en/), supplemented by other sources, such as data from:
- interviews conducted during the project, 
- ministries and governmental agencies (data from websites or retrieved on formal request),
- desk research results (data from the documents). 

In all of above mentioned cases, it was neccessary to transform the data from the original formats into one csv's with following common data structure:  
- column "gmina_name" with the name of the local community,
- column "date" with the indication of the date, when observation has been made (one year granulation starting with 01.01.1995 with one-year step),
- column(s) named after variable(s) name(s) populated with values registered for the variable(s) in the communities on various dates ("observations").   

In result, 33 .csv files have been created and filled with data (depending on the data source, this process was more or less automated). The initial source' data catalogue includes: 
- 1 file with data derived from interviews,
- 3 files with data derived from desk research (documents)
- 1 file with data derived from ministries / agencies, 
- 28 files with data derived from Central Statistical Office.

Data included in the csv files covers various timespans and geographical scope, dependent on the variables. 

All above mentioned .csv files are stored in relevant directory of this repository [(see here)](https://github.com/cam48eco/LifeDWH/tree/main/data/observations). The data are reviewed and updated periodically (at least once per month) manually. In case the new observations are detected by research team, the relevant .csv file is updated in the repository. In case if the new data source with new data will appear, the new .csv file is created and pushed in the repository.

An additional data source is a s_multi_dimension_gmina.csv file with data describing features of each out of 93 communities. The file is stored in [separate directory](https://github.com/cam48eco/LifeDWH/tree/main/data/communities). 

## II. ETL and Data warehouse design and implementation


### 2.1. Preparatory tasks  

#### 2.1.1. Data transformation 

For the use of data included in the 33 csv files with initial data (excluding s_multi_dimension_gmina.csv) for data warehouse purposes, it was neccessary to conduct an additional transformation to receive a separate tables in separate .csv files for each of the variables, adding the variable name as a name of newly created csv file. To avoid time-consumig work during this job, the script in python (see below) has been implemented, to automatize the process of creation of separate file for each variable. 

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
                newfiletemp.to_csv('D:\Path\To\csv\files\AfterSplit\{}.csv'.format(datafile.columns[i]), sep = ";", index=False) 

splitingfile()

```
In result 33 files with initial data were transformed into 126 separate csv files (each file for separate variable, the filename named after variable, unified structure: three columns: "gmina_name", "date" and "value"), and stored in separated part of this repository [here](https://github.com/cam48eco/LifeDWH/tree/main/data/observationssplit). The process is repeated every time when at least one, from the initial 33 files with data, is changing.  

 
#### 2.1.2. Creation and feeding of 'sources' database for storing 'transactional' data 

To ensure the smooth functioning of data warehouse, it was neccessary to design and set-up a database, where 'transactional' data, being stored so far in flat files (.csv; see previous chapter) will be stored. 
The term 'transactional' relates in this specific case to observations' data with values of various variables on various dates. The database (updated periodically throghout Airflow task, what will be discussed in the next section), serves as a 'point of departure' for ETL process for the data warehouse. 

The database is very simple and consist out of tables with observations data (126 tables) and one table with specific information on communities - in both cases filled periodically from flat .csv files. 

The steps for creation and feeding of OLTP database in the SQL Server environment (SSMS) are described below. 


**Sources database creation**

![OltpLogo](https://github.com/cam48eco/LifeDWH/blob/main/img/CreateOLTP.png)


**Database tables creation and data fetching** 

Creation and fetching source database tables with observations data (126 tables) and information on communities (1 table) from the csv's is conducted with Airflow task (daily executed to catch possible changes in any of 126 tables; the information on communities table is fixed), using DAG with [mssql operator](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/operators.html), allowing the execution of SQL server queries on the database. 

The core of the approach is the T-SQL script, focused not only on the initial tables creation and fetching, but enabling database tables update (with duplications preventing), as well. As the T-SQL code is long, it has been not included into [respective DAG](https://github.com/cam48eco/LifeDWH/tree/main/dags/C0_dblife_insertDataIntoSourceTables.py), but the execution of the [separate sql file](https://github.com/cam48eco/LifeDWH/tree/main/dags/C0_insertDataIntoSourceTables_from_csvs.sql) from within DAG has been chosen (see DAG line 42), what makes the DAG code more clear. 
Above mentioned approach is assumed for entire solution and all separated sql files with T-SQL scripts (toghetger with DAgs) are stored [here](https://github.com/cam48eco/LifeDWH/tree/main/dags). 

"Import flat files" feature present in SSMS was not useful to achieve above mentioned resuls, as it serves only single files. SSMS Express version, used for this solution, does not include task scheduling feature with SQL Server Job Agent, so preparation of dedicated T-SQL code and matching it with Airflow was neccessary. Another option, to consider in the future is to deploy [SQL Server triggers](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-trigger-transact-sql?view=sql-server-ver16). 

It has to be mentioned, that to ensure proper execution of connection between Airflow and SQL Server, one schould ensure that Airflow is supplemented with the relevant plugin enabling MsSqlOperator. If not, provision following command in CMD is neccessary:

```bash
airflow connections add [mssql_conn_id_name] --conn-uri mssql://sa:[password]@[sever_ip]:[port]
```

In result, the relevant option will appear in Airflow in Admin -> Connections panel: 

![AirflowMssql](https://github.com/cam48eco/LifeDWH/blob/main/img/Airflow_Mssql.png)






















 






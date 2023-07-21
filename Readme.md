![LifeLogo](https://github.com/cam48eco/LifeDWH/blob/main/img/LIFE.png)

# ETL solution for Life

## Motivation 
The motivation to prepare the solution was a need to facilitate the data flow and ensuring access to data neccessary for analyses of socio-economic aspects of Pilica catchment area, consisting out of 93 communities  (gmina) on NUTS5 level.
Previously, the dedicated tool [(R package)](https://github.com/ap48atgazetadotpl/R_GUS_retrieving) for automated data retrieval from Central Statistical Office (GUS) in Poland, based on GUS API, has been elaborated, however the work has been suspended because of integration limitations. 

## Assumptions
The point of departure for solution preparation was a statement, that the solution should be deployed in a way, assuming usage of on-premise infrastructure, including an server working on local machine (e.g. laptop) connected to web, due to lack of financial resources for estabilishment and maitenance of external server or cloud solutions. In result, free versions of software (SSMS Express) have to be used, limiting the access to useful features, in particular, automated task schedulling in database itself. 
The solution' architecture follows these limitations, and in consequence tasks, automatizing data flows, were implemented with in-house T-SQL scripts / Airflow/python DAGs.  
The added value of the solution is its scalability and adaptability, enabling after minor changes, preparation a similar solutions in on-premise servers or (with more effort) in the cloud. Disregard on above mentioned limits, some of cloud solutions have been also discussed and implemented (in particular for visualization), to take the adventages from such kind of technologies as Tableau and Looker. 

## Stack

- Windows 10
- Google Drive
- SQL Server 2017 
- SSMS 17.9.1
- Airflow under Debian (in Win10 environment; setup see [info](https://www.freecodecamp.org/news/install-apache-airflow-on-windows-without-docker/))

## Scope and architecure design
The repository documents the steps and include resources used for solution creation and set up of data warehouse (under SQL Server 2017) and its maitenance with orchestration tool (Airflow).
The following issues are covered:
- I. Data sources identification, preprocessing and storage 
- II. ETL and Data warehouse design and implementation
- III. Automation of the ETL process with Airflow.

Fig. 1. Brief presentation of the pipeline / ETL architecture assumptions
![LifeBPM](https://github.com/cam48eco/LifeDWH/blob/main/img/diagram_Airflow.svg)

Source: Author own elaboration 

The solution undergoes a continuos development, with the assumption, that new sources of raw data could appear (for example a real time data retrieved from sensors, throghout APIs, etc.). In such cases, the solution will be adjusted to ensure utilization of these data (for example throgout creation and deployment of new scripts) and flat data sources, presented above will be enriched by another data types.  


## I. Data sources identification, initial preprocessing and storage 

According to project assumptions, the main data source was a Central Statistical Office of Republic of Poland - Local Data Bank (https://stat.gov.pl/en/), supplemented by other sources, such as data from:
- interviews conducted during the project, 
- ministries and governmental agencies (data from websites or retrieved on formal request),
- desk research results (data from the documents). 

In all of above mentioned cases, it was neccessary to transform the data from the original formats into csv's with following common data structure:  
- column "gmina_name" with the name of the local community,
- column "date" with the indication of the date, when observation has been made (one year granulation starting with 01.01.1995 with one-year step),
- column(s) named after variable(s) name(s) populated with values registered for the variable(s) in the communities on various dates ("observations").   

In result, 33 .csv files have been created and filled with data (depending on the data source, this process was more or less automated). The initial source' data catalogue includes: 
- 1 file with data derived from interviews,
- 3 files with data derived from desk research (documents)
- 1 file with data derived from ministries / agencies, 
- 28 files with data derived from Central Statistical Office.

Data included in the csv files covers various timespans and geographical scope, dependent on the variables. 

All above mentioned .csv files are stored on Google Drive, publicly available [(see here)](https://drive.google.com/drive/folders/1vit8l2RQdz1xgrHEYeqUuz4MB73s53G5), as 'observations'. Data are reviewed and updated periodically (at least once per month) manually. 
In case the new observations are detected by research team in existing data sources, the relevant .csv file is updated in the repository. 
In case if the new flat data source with new data would appear, the new .csv file would be created and pushed into the repository.
Primarily, the repository has been stored on github, but eventually, the Google Drive has been choosen as a storage. 

An additional data source is a s_multi_dimension_gmina.csv file with data describing features of each out of 93 communities. The file is stored in [separate directory](https://drive.google.com/drive/folders/1zJvBEfYQirQH3WtjSt_KS_GfFxMSXXqo). 

Every day two above mentioned Google Drive folders ('observations', 'communities') are migrated into on-premise machine with the [first  Airflow DAG](https://github.com/cam48eco/LifeDWH/blob/main/dags/C1_retrieveFoldersPreSourceDataFromGoogleSheets.py) - "C1_retrieveFoldersPreSourceDataFromGoogleSheets", with the aim to serve as a prerequisite for next steps of pipeline and eventually, ETL.  
The choice of Google Drive was connected with the idea to avoid registration of the project and charges (for example at GCP, where 'enable billing' option should be activated in this case). 
Eventually, Google Drive along with the library of [gdown](https://github.com/wkentaro/gdown) supporting DAG' retrieving fuction has been choosen, as it enables retrieval of files (and folders) without strong size restictions, what can be a of importance in the future. 

## II. ETL and Data warehouse design and implementation


### 2.1. Preparatory tasks  

#### 2.1.1. First data transformation 

For the use of data included in the 33 csv files with initial data (excluding s_multi_dimension_gmina.csv) for data warehouse purposes, it is  neccessary to conduct an additional transformation to receive a separate .csvs for each of the variables, present in the files (majority out of 33 files includes data on more than one variable), naming the newly created csv file after the variable name. To avoid time-consumig work during this job, and to automate it, the script in python (see below) has been prepared and integrated in the [next DAG](https://github.com/cam48eco/LifeDWH/blob/main/dags/C2_transformFolderPreResourceDataIntoSplit.py) - "C2_transformFolderPreResourceDataIntoSplit". 

In result, 33 files with 'observations', after migration from Google Drive to the on-premise device, are transformed (splitted) and stored on local device in 'observations' directory as 126 separate csv files (each file for separate variable, the filename named after variable, unified structure: three columns: "gmina_name", "date" and "value").  
In the future, the process of transformation of 33 'observation' files into splitted 126 files will be repeated every time when the change of at least one of 33 files will be detected (with the use of Airflow' SensorOperator). 
At the moment, the transformation from 33 into 126 files is conducted automatically (timespan of 4 minutes) after first DAG (33 csv files retrieval from Google Drive) is completed. 

 
#### 2.1.2. Creation and feeding of 'sources database' for storing 'transactional' data 

To ensure the smooth functioning of data warehouse, it was neccessary to design and set-up a 'sources database' to store 'transactional' data, being stored so far in .csv flat files (see previous chapter).  
The term 'transactional' relates in this specific case to observations' data with values of various variables on various dates. The database (updated periodically throghout Airflow task, what will be discussed in the next section), serves as a 'point of departure' for ETL process for the data warehouse. 

The 'sources database' (namely in this case: 'oltplifesources') is very simple and consist out of tables with observations data (126 tables) and one table with specific information on communities - in both cases filled periodically migrating from flat .csv files, stored respectivelly in directories: 'observations' and 'communities'. 
In case if new data sources would appear, the database will be reconstructed to ensure new data utilisation.  

The steps for creation and feeding of 'sources database' (being in fact an OLTP databse) in the SQL Server environment (SSMS) are described below. 


**'Sources database' creation**

The process has been developed in SSMS (see below).

![OltpLogo](https://github.com/cam48eco/LifeDWH/blob/main/img/CreateOLTP.png)

In addition, apart from the default SQL Server .dbo schema (for 'observations' tables), an additional schema .dim (for 'community' table) has been created. 

![OltpLogo](https://github.com/cam48eco/LifeDWH/blob/main/img/CreateSchema.png)



**'Sources database' tables creation and data fetching throghout migration from .csvs** 

Creation and fetching source database tables in schema .dbo with 'observations' data (126 tables) and information on 'communities' in .dim schema (1 table) from the csv's is conducted with two separate Airflow tasks (daily executed to catch possible changes in any of the tables), using DAGs with [mssql operator](https://airflow.apache.org/docs/apache-airflow-providers-microsoft-mssql/stable/operators.html), allowing the execution of SQL server queries on the database. 

The core of the approach are T-SQL scripts, focused not only on the initial tables creation and fetching, but enabling database tables update (with duplications preventing), as well. As the T-SQL codes are long, they have been not included into respective DAGs: 
- [first](https://github.com/cam48eco/LifeDWH/blob/main/dags/C4_insertDataIntoOltplifesourcesDboSourceTables_from_csvs.py) - responsible for fetching the oltpsources database, schema 'dbo', with 126 .csv tables with 'observations'
- [second](https://github.com/cam48eco/LifeDWH/blob/main/dags/C3_insertDataIntoOltplifesourcesDimSourceTables_from_csvs.py) - responsible for fetching the oltpsources database, schema 'dim', with 1 .csv table with 'communities' details. 

,but each DAG is supplemented by relevant t-sql file, to ensure the clarity: 
- [T-sql script for first case:](https://github.com/cam48eco/LifeDWH/blob/main/dags/C4_insertDataIntoOltplifesourcesDboSourceTables_from_csvs.sql) for fetching oltplife tables with observations, 
- at [T-sql script for second case:](https://github.com/cam48eco/LifeDWH/blob/main/dags/C3_insertDataIntoOltplifesourcesDimSourceTables_from_csvs.sql) for fetching oltplife table with communities details.  

Above mentioned approach was assumed for entire solution; all DAGs with accompanying T-SQL scripts are stored [here](https://github.com/cam48eco/LifeDWH/tree/main/dags). 

It has to be mentioned, that "Import flat files" feature present in SSMS was not useful to achieve above mentioned resuls, as it serves only single files while migrating into SQL Server database. In addition, SSMS Express version, used for this solution, does not include task scheduling feature with SQL Server Job Agent, so preparation of dedicated T-SQL code and matching it with Airflow was neccessary. Another option, to consider in the future is to deploy [SQL Server triggers](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-trigger-transact-sql?view=sql-server-ver16). 

It has to be mentioned  as well, that to ensure proper execution of connection between Airflow and SQL Server, it was neccessary to supplement Airflow with the relevant plugin enabling MsSqlOperator. For this purpose, provision following command in CMD was neccessary:

```bash
airflow connections add [mssql_conn_id_name] --conn-uri mssql://sa:[password]@[sever_ip]:[port]
```

In result, the relevant option appeared in Airflow in Admin -> Connections panel, enabling use of MsSqlOperator in DAGs, toghether with attribute of connection (mssql_conn_id="[mssql_conn_id_name]").

![AirflowMssql](https://github.com/cam48eco/LifeDWH/blob/main/img/Airflow_Mssql.png)


#### 2.1.3. 'Staging database' and its tables creation with fetching with data transformed from 'Sources database' 

The second database - staging database (namely: 'oltplifestaging') has been designed to support transformations of data from 'source' stage to the form adequate for dimensions extraction and data warehouse feeding. This functionality will be useful in the future as well, when new, real time data as data sources will appear. 
It was assumed, that after transformations into 'staging' database, the 'sources' database tables are to be dropped, as there is an 'initial' backup in the form of Google drive files. 
The special case, will be - if appears - the neccessity to drop (after transformation / aggregation and after data warehouse feeding) the 'Sources database' with the real time data (for example in the form of InfluxDB) to avoid inefficient use of disk space (according to bucket philosophy - retention policy for database). 

![OltpLogo](https://github.com/cam48eco/LifeDWH/blob/main/img/CreateOLTPstaging.png)

According to above mentioned assumptions, in the case of tables with data on: 'observations' and 'communities', the tables in oltplifestaging are feeded with tables from oltplifesources database with respective [DAG](https://github.com/cam48eco/LifeDWH/blob/main/dags/C5_insertDataFromOltplifesourcesIntoOltplifeStaging.py) and accompanying [T-SQL](https://github.com/cam48eco/LifeDWH/blob/main/dags/C5_insertDataFromOltplifesourcesIntoOltplifeStaging.sql) with some minor transformations.
As mentioned above, in the future, in the case of other sources as real time data appear, new, respective DAGs will be elaborated to transform the data from oltplifesources when transfering to respective tables in oltplifestaging. 
The assumptions for the processes connected with this aspects have been elaborated and presented [here](https://github.com/cam48eco/LifeDWH/blob/main/img/sources_with_RT.svg). 






























 






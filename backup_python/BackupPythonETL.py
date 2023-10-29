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

# Path with observations: C:\Users\dell\Desktop\DWHonPremise\observations

# Exit python and go again

import pandas as pd
import datetime as dt
import gdown

def retrievecommunitiesdatafromgoogle ():
    import gdown
    # Url for google drive directory with 1 file with communities details. Written into /home/pawelc/communities.
    url = "https://drive.google.com/drive/folders/1zJvBEfYQirQH3WtjSt_KS_GfFxMSXXqo"
    gdown.download_folder(url, quiet=True, use_cookies=False)

retrievecommunitiesdatafromgoogle()

# Transform data from csv (C2) transformFolderPreResourceDataIntoSplit.py

def splitingfile():
	import pandas as pd
	import os 
	# Create list of csv files with data indicating directory where the files are stored  
	datafilelist = os.listdir('C:/Users/dell/Desktop/DWHonPremise/observations')
	# Create intial dataframe (from the first csv dataset) with columns: 'community names' and 'dates of observation' when 'observation' folder not empty 
	if len(datafilelist)!=0:
		dftemp = pd.read_csv('C:/Users/dell/Desktop/DWHonPremise/observations/{}'.format(datafilelist[1]), sep = ";")  
		dftemp =  dftemp[['gmina_name', 'date']]
		# Splitting data from every .csv file from columns to separate csv files - one for each variable  
		# In result 33 .csv files will be transformed into 126 separate .csv files in 'observationsaftersplit' directory (each file for separate variable, the filename named after variable, unified structure). 
		# # Iterate over every file in the directory with .csv files 
		for element in datafilelist:
			if 'csv' in element:
				# Read .csv as dataframe
				datafile = pd.read_csv('C:/Users/dell/Desktop/DWHonPremise/observations/{}'.format(element), sep = ";")
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
					newfiletemp.to_csv('C:/Users/dell/Desktop/DWHonPremise/observationsaftersplit/{}.csv'.format(datafile.columns[i]), sep = ";", index=False) 

# Insert into DIM tables - C3_insertDataIntoOltplifesourcesDimSourceTables_from_csv
# SQL code to be executed in SQL database (SSMS)

splitingfile()

-- Select datbase

USE oltplifesources

-- Create temporary table #csvlist to store the list of csv files with data  

IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dim' and t.name = '#csvlist')
CREATE TABLE [dbo].[#csvlist]([id] INT IDENTITY(1,1), [filenames] nvarchar(50), [depth] nvarchar(50), [csvfile] nvarchar(50))

-- Identify the csv files with data and feed #csvlist table 
INSERT INTO [dbo].[#csvlist]([filenames], [depth], [csvfile]) EXEC master.dbo.xp_dirTree '/home/path/communities', 1, 1;

-- Transform #csvlist table to get tables names from .csv filesnames skipping .csv endings and drop unnecessary columns 
ALTER TABLE [dbo].[#csvlist] 
ADD tablenames AS LEFT(filenames,LEN(filenames)-4)

-- Declare variables 

DECLARE @Counter INT
DECLARE @filename nvarchar(50)
DECLARE @tablename nvarchar(50)
DECLARE @csvpath nvarchar(max)
DECLARE @sqldrop nvarchar(max)
DECLARE @sqlcreate nvarchar(max)
DECLARE @sqlinsert nvarchar(max)

-- Set initial variables values 
SET @Counter=1
SET @filename= (SELECT filenames FROM [dbo].[#csvlist] WHERE id = 1)
SET @tablename= (SELECT tablenames FROM [dbo].[#csvlist] WHERE id = 1)
SET @csvpath= '/home/path/communities/' + (SELECT filenames FROM [dbo].[#csvlist] WHERE id = 1)

-- Repeat until the last row in the #csvlist table is reached 
WHILE ( @Counter <= (select COUNT(*) from [dim].[#csvlist]))
BEGIN
	-- Assign the name of csv file, table and path according to id 
	SET @filename= (SELECT filenames FROM [dbo].[#csvlist] WHERE id = @Counter)
	SET @tablename= (SELECT tablenames FROM [dbo].[#csvlist] WHERE id = @Counter)
	SET @csvpath = '/home/path/communities/' + (SELECT filenames FROM [dbo].[#csvlist] WHERE id = @Counter)
	-- Assign queries to be executed 
	SET @sqldrop= 'DROP TABLE [dim].' + QUOTENAME(@tablename) + ';'
	SET @sqlcreate= 'CREATE TABLE [dim].' + QUOTENAME(@tablename) + '([gmina_id] nvarchar(10), [gmina_name] nvarchar(50), [gmina_powiat_name] nvarchar(50), [gmina_region_name] nvarchar(50), [gmina_stat_code] nvarchar(50), [gmina_pid] nvarchar(50), [gmina_sid] nvarchar(50), [gmina_attrib1] nvarchar(50), [gmina_attrib2] nvarchar(50));'
	SET @sqlinsert= 'BULK INSERT [dim].' + QUOTENAME(@tablename) + 'FROM' + QUOTENAME(@csvpath) + 'WITH (FIRSTROW = 2, FORMAT = ''CSV'', FIELDTERMINATOR = '';'' , FIELDQUOTE = ''"'');'
	-- Execute table creation if not exists and insert data from relevant csv file 
	USE oltplifesources
	IF EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dim' and t.name = (SELECT tablenames FROM [dim].[#csvlist] WHERE tablenames = @tablename))
		EXEC (@sqldrop)
		EXEC (@sqlcreate)
		EXEC (@sqlinsert)
	SET @Counter  = @Counter  + 1
END


# Insert data into source tables - C4_insertDataIntoOltplifesourcesDboSourceTables_from_csvs (C4)
# SQL to be executed in database (SSMS)

-- Select database

USE oltplifesources

-- Create temporary table #csvlist to store the list of csv files with data  

IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = '#csvlist')
CREATE TABLE [dbo].[#csvlist]([id] INT IDENTITY(1,1), [filenames] nvarchar(50), [depth] nvarchar(50), [csvfile] nvarchar(50))

-- Identify the csv files with data and feed #csvlist table 
INSERT INTO [dbo].[#csvlist]([filenames], [depth], [csvfile]) EXEC master.dbo.xp_dirTree '/home/path/observationsaftersplit', 1, 1;

-- Transform #csvlist table to get tables names from .csv filesnames skipping .csv endings and drop unnecessary columns 
ALTER TABLE [dbo].[#csvlist] 
ADD tablenames AS LEFT(filenames,LEN(filenames)-4)

-- Declare variables 

DECLARE @Counter INT
DECLARE @filename nvarchar(50)
DECLARE @tablename nvarchar(50)
DECLARE @csvpath nvarchar(max)
DECLARE @sqldrop nvarchar(max)
DECLARE @sqlcreate nvarchar(max)
DECLARE @sqlinsert nvarchar(max)

-- Set initial variables values 
SET @Counter=1
SET @filename= (SELECT filenames FROM [dbo].[#csvlist] WHERE id = 1)
SET @tablename= (SELECT tablenames FROM [dbo].[#csvlist] WHERE id = 1)
SET @csvpath= '/home/path/observationsaftersplit/' + (SELECT filenames FROM [dbo].[#csvlist] WHERE id = 1)

-- Repeat until the last row in the #csvlist table is reached 
WHILE ( @Counter <= (select COUNT(*) from [dbo].[#csvlist]))
BEGIN
	-- Assign the name of csv file, table and path according to id 
	SET @filename= (SELECT filenames FROM [dbo].[#csvlist] WHERE id = @Counter)
	SET @tablename= (SELECT tablenames FROM [dbo].[#csvlist] WHERE id = @Counter)
	SET @csvpath = '/home/path/observationsaftersplit/' + (SELECT filenames FROM [dbo].[#csvlist] WHERE id = @Counter)
	-- Assign queries to be executed 
	SET @sqldrop= 'DROP TABLE [dbo].' + QUOTENAME(@tablename) + ';'
	SET @sqlcreate= 'CREATE TABLE [dbo].' + QUOTENAME(@tablename) + '([gmina_name] nvarchar(50), [date] nvarchar(50), [value] nvarchar(150));'
	SET @sqlinsert= 'BULK INSERT [dbo].' + QUOTENAME(@tablename) + 'FROM' + QUOTENAME(@csvpath) + 'WITH (FIRSTROW = 2, FORMAT = ''CSV'', FIELDTERMINATOR = '';'' , FIELDQUOTE = ''"'');'
	-- Execute table creation if not exists and insert data from relevant csv file 
	USE oltplifesources
	IF EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = (SELECT tablenames FROM [dbo].[#csvlist] WHERE tablenames = @tablename))
		EXEC (@sqldrop)
		EXEC (@sqlcreate)
		EXEC (@sqlinsert)
	SET @Counter  = @Counter  + 1
END

# C5_insertDataFromOltplifesourcesIntoOltplifeStaging
# Inserting into staging from source 
# SQL to be executed in database 

/****** Script for C5  once / day ******/


-- Drop temporary table with the list of 'source' tables if exists and create a new one filling with the list of 'source' tables 

	-- Select database
USE oltplifesources
	-- Drop temporary table '#sourcetables' with the list of 'source' tables if exists 
IF EXISTS (SELECT * FROM tempdb.sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name LIKE '#sourcetables%') 
	DROP TABLE [dbo].[#sourcetables]
IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name LIKE '#sourcetables')
	CREATE TABLE [dbo].[#sourcetables]([id] INT IDENTITY(1,1), [tablesnames] nvarchar(50))
	-- Identify list of source tables from sys.tables and feed table '#sourcetables' table with 'source' tables names
INSERT INTO [dbo].[#sourcetables]([tablesnames]) SELECT name FROM sys.tables t where schema_name(t.schema_id) = 'dbo';


-- Create and feed table 'observations' with merged all 'source' observation tables 

	-- Select database
USE oltplifestaging
	-- Create empty table 'observations' in oltplifestaging as seed 
IF EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'observations') 
	DROP TABLE [oltplifestaging].[dbo].[observations]
IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'observations') 
	CREATE TABLE[oltplifestaging].[dbo].[observations]([id] INT IDENTITY(1,1), [gmina_name] [nvarchar](50) NULL, [date] [nvarchar](50) NULL, [value] [nvarchar](150) NULL)

	-- First step - initial feeding
	-- Declare variables 
DECLARE @Counter INT
DECLARE @tablename nvarchar(50)
DECLARE @sqlinsert nvarchar(max)

	-- Set initial variables values 
SET @Counter=1 -- Column '1' as initial 
SET @tablename= (SELECT tablesnames FROM [dbo].[#sourcetables] WHERE id = 1)

	-- Take the 'observations' table as a seed and feed initial (first) column with values from first 'source' table from oltplifesources, renaming the column name: from 'value' after 'source' table name
SET @tablename= (SELECT tablesnames FROM [dbo].[#sourcetables] WHERE id = @Counter)
SET @sqlinsert = 'INSERT INTO [oltplifestaging].[dbo].[observations] (gmina_name, date, value) SELECT  gmina_name, date, value FROM [oltplifesources].[dbo].' + QUOTENAME(@tablename)
EXEC (@sqlinsert)
EXEC sys.sp_rename
	@objname = N'oltplifestaging.dbo.observations.value', 
	@newname = @tablename, 
    @objtype = 'COLUMN' 


	-- Second step - feeding 'observations' with remain source tables
	-- Declare variable for update the rest tables from oltplifesources 
DECLARE @sqlupdate nvarchar(max)

	-- Set initial variables values 
	SET @Counter=2 -- Column '2' as initial for remaining columns 
	
	-- Repeat feeding 'observations' with rest of 'source' tables until last row in the #sourcetables table is reached - merging the seed 'observations' table with other tables renaming the column name 
WHILE (@Counter <= (select COUNT(*) from [dbo].[#sourcetables]))
BEGIN
	-- Assign the name of the table 
	SET @tablename= (SELECT tablesnames FROM [dbo].[#sourcetables] WHERE id = @Counter)
	-- Assign query to be executed. 
	-- Add a new column with the name of the next table to be attached
	SET @sqlinsert = 'ALTER TABLE [oltplifestaging].[dbo].[observations] ADD' + QUOTENAME(@tablename) + 'NVARCHAR(150)'  
	EXEC (@sqlinsert)
	--SET @sqlinsert = 'INSERT INTO [oltplifestaging].[dbo].[observations] (' +  QUOTENAME(@tablename) + ') SELECT value FROM [oltplifesources].[dbo].' + QUOTENAME(@tablename)
	-- UPDATE the newly created column instead of 'INSERT', as 'insert' adds new rows in each loop making table not eligible. 
	SET @sqlupdate = 
	'UPDATE [oltplifestaging].[dbo].[observations] 
	SET 
	[oltplifestaging].[dbo].[observations].' + QUOTENAME(@tablename) + '= [oltplifesources].[dbo].' + QUOTENAME(@tablename) + '.value
	FROM [oltplifestaging].[dbo].[observations] 
	INNER JOIN
	[oltplifesources].[dbo].' + QUOTENAME(@tablename) + 
	'ON [oltplifestaging].[dbo].[observations].date = [oltplifesources].[dbo].' + QUOTENAME(@tablename) + '.date AND [oltplifestaging].[dbo].[observations].gmina_name = [oltplifesources].[dbo].' + QUOTENAME(@tablename) + '.gmina_name'
	EXEC (@sqlupdate)
	SET @Counter  = @Counter  + 1
END



-- Recreate dim.s_multi_dimension_gmina table from oltplifesources into oltplifestaging

-- Select (change) database
USE oltplifestaging
-- Create empty table 's_multi_dimension_gmina' in oltplifestaging   
IF EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dim' and t.name = 's_multi_dimension_gmina') 
	DROP TABLE [oltplifestaging].[dim].[s_multi_dimension_gmina]
IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dim' and t.name = 's_multi_dimension_gmina') 
	CREATE TABLE[oltplifestaging].[dim].[s_multi_dimension_gmina]([id] INT IDENTITY(1,1), [gmina_id] [nvarchar](50) NULL, [gmina_name] [nvarchar](50) NULL, [gmina_powiat_name] [nvarchar](50) NULL, 
      [gmina_region_name][nvarchar](50) NULL,
      [gmina_stat_code] [nvarchar](50) NULL,
      [gmina_pid] [nvarchar](50) NULL,
      [gmina_sid] [nvarchar](50) NULL,
      [gmina_attrib1] [nvarchar](50) NULL,
      [gmina_attrib2] [nvarchar](50) NULL)
-- Copy data from [oltplifesources].[dim].[s_multi_dimension_gmina]  into [oltplifestaging].[dim].[s_multi_dimension_gmina]
INSERT INTO [oltplifestaging].[dim].[s_multi_dimension_gmina](gmina_id, gmina_name, gmina_powiat_name, gmina_region_name, gmina_stat_code, gmina_pid, gmina_sid, gmina_attrib1, gmina_attrib2) SELECT * FROM [oltplifesources].[dim].[s_multi_dimension_gmina]
 
  

# C6_Create_Feed_DimTables

/****** C6 once / week   ******/
 
-- Create dbo.DimGmina table and feed it with data from respective table from oltplifestaging database (i.e.  dim.s_multi_dimension_gmina)

-- Select database
USE dwlife

-- Create empty table 'DimGmina' in dwlife if not exists  
IF EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'DimGmina') 
	DROP TABLE [dwlife].[dbo].[DimGmina]
IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'DimGmina') 
	CREATE TABLE[dwlife].[dbo].[DimGmina](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[gmina_id] [nvarchar](50) NULL,
	[gmina_name] [nvarchar](50) NULL,
	[gmina_powiat_name] [nvarchar](50) NULL,
	[gmina_region_name] [nvarchar](50) NULL,
	[gmina_stat_code] [nvarchar](50) NULL,
	[gmina_pid] [nvarchar](50) NULL,
	[gmina_sid] [nvarchar](50) NULL,
	[gmina_attrib1] [nvarchar](50) NULL,
	[gmina_attrib2] [nvarchar](50) NULL
) ON [PRIMARY]

-- Copy data from [oltplifestaging].[dim].[s_multi_dimension_gmina]  into [dwlife].[dbo].[DimGmina]. There is no need to update as the values comes from source
INSERT INTO [dwlife].[dbo].[DimGmina](gmina_id, gmina_name, gmina_powiat_name, gmina_region_name, gmina_stat_code, gmina_pid, gmina_sid, gmina_attrib1, gmina_attrib2) SELECT gmina_id, gmina_name, gmina_powiat_name, gmina_region_name, gmina_stat_code, gmina_pid, gmina_sid, gmina_attrib1, gmina_attrib2 FROM [oltplifestaging].[dim].[s_multi_dimension_gmina]
 

-- Create empty table 'DimYear' in dwlife if not exists  
IF EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'DimYear') 
	DROP TABLE [dwlife].[dbo].[DimYear]
IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'DimYear') 
	CREATE TABLE[dwlife].[dbo].[DimYear](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[year] [nvarchar](50) NULL
) ON [PRIMARY]

-- Create values for 'year' variable, extracting it  from [oltplifestaging].[dbo].[observations]   
DECLARE @sqlinsert nvarchar(max)
-- Insert unique 'year' values into newly created table 
SET @sqlinsert = 'INSERT INTO [dwlife].[dbo].[DimYear] (year) SELECT DISTINCT date FROM [oltplifestaging].[dbo].[observations] ORDER BY date ASC'
EXEC (@sqlinsert)
 

 # C7_Create_Feed_FactTable

 -- Select database

USE dwlife

-- Drop temporary table #FactTablePopulation if exists and create a new one to store temporary year, gmina_name and value of [variable] 
IF EXISTS (SELECT * FROM tempdb.sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name LIKE '#FactTablePopulation%') 
	DROP TABLE [dbo].[#FactTablePopulation]
IF NOT EXISTS (SELECT * FROM tempdb.sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name LIKE '#FactTablePopulation')
	CREATE TABLE [dbo].[#FactTablePopulation]([id] INT IDENTITY(1,1), [year] [nvarchar](50) NULL, [year_fk] [nvarchar](50) NULL, [gmina_name] [nvarchar](50) NULL, gmina_fk [nvarchar](50) NULL, [population] [float] NULL) 
-- Insert data: year, gmina_name, [variable] from [oltplifestaging].[dbo].[observations] into #FactTablePopulation
INSERT INTO [dwlife].[dbo].[#FactTablePopulation] (year, gmina_name, population) SELECT date, gmina_name, ludnosc FROM [oltplifestaging].[dbo].[observations] 


-- Drop table FactTable[Variable] if exists  
IF EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'FactTablePopulation') 
	DROP TABLE [dbo].[FactTablePopulation]
-- Create FactTable[Variable] by: leftjoin DimYear, DimGmina to FactTablePopulation from #FactTablePopulation, to feed *._fk colums and to update FactTablePopulation with join results. 
-- Query results with immediate creation of FactTablePopulation table even SELECT is implemented; no CREATE TABLE is needed.  
SELECT y.id as year_fk, g.id as gmina_fk, p.population INTO [dwlife].[dbo].[FactTablePopulation] FROM [dwlife].[dbo].[#FactTablePopulation] p JOIN [dwlife].[dbo].[DimGmina] g ON p.gmina_name = g.gmina_name JOIN [dwlife].[dbo].[DimYear] y ON p.year = y.year 
-- Add computed column to fact table
ALTER TABLE [dwlife].[dbo].[FactTablePopulation] ADD populationdouble AS population * 2   









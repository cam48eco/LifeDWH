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
 
  
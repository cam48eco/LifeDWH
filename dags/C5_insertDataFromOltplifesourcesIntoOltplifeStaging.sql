
-- Select database
USE oltplifesources

-- Drop temporary table with the list of 'source' tables if exists and create a new one 
IF EXISTS (SELECT * FROM tempdb.sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name LIKE '#sourcetables%') 
	DROP TABLE [dbo].[#sourcetables]
IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name LIKE '#sourcetables')
	CREATE TABLE [dbo].[#sourcetables]([id] INT IDENTITY(1,1), [tablesnames] nvarchar(50))

-- Identify source tables and feed #sourcetables table with source tables names
INSERT INTO [dbo].[#sourcetables]([tablesnames]) SELECT name FROM sys.tables t where schema_name(t.schema_id) = 'dbo';

-- Select (change) database
USE oltplifestaging

-- Create empty table 'observations' in oltplifestaging as seed 
IF EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'observations') 
	DROP TABLE [oltplifestaging].[dbo].[observations]
IF NOT EXISTS (SELECT * FROM sys.tables t join sys.schemas s on (t.schema_id = s.schema_id) where s.name = 'dbo' and t.name = 'observations') 
	CREATE TABLE[oltplifestaging].[dbo].[observations]([id] INT IDENTITY(1,1), [gmina_name] [nvarchar](50) NULL, [date] [nvarchar](50) NULL, [value] [nvarchar](150) NULL)

-- Declare variables 
DECLARE @Counter INT
DECLARE @tablename nvarchar(50)
DECLARE @sqlinsert nvarchar(max)

-- Set initial variables values 
SET @Counter=1
SET @tablename= (SELECT tablesnames FROM [dbo].[#sourcetables] WHERE id = 1)

-- Take the 'observations' table as a seed and feed initial column with values from first table from oltplifesources, changing the column name from 'value' into table name
SET @tablename= (SELECT tablesnames FROM [dbo].[#sourcetables] WHERE id = @Counter)
SET @sqlinsert = 'INSERT INTO [oltplifestaging].[dbo].[observations] (gmina_name, date, value) SELECT  gmina_name, date, value FROM [oltplifesources].[dbo].' + QUOTENAME(@tablename)
EXEC (@sqlinsert)
EXEC sys.sp_rename
	@objname = N'oltplifestaging.dbo.observations.value', 
	@newname = @tablename, 
    @objtype = 'COLUMN' 


-- Declare variable for update the rest tables from oltplifesources 
DECLARE @sqlupdate nvarchar(max)

-- Set initial variables values 
SET @Counter=2
-- Repeat for the rest of oltplifesource tables until last row in the #sourcetables table is reached - merging the seed 'observations' table with other tables changing the column name 
-- WHILE (@Counter <= (select COUNT(*) from [dbo].[#sourcetables]))
WHILE (@COUNTER <=4)
BEGIN
	-- Assign the name of the table 
	SET @tablename= (SELECT tablesnames FROM [dbo].[#sourcetables] WHERE id = @Counter)
	-- Assign query to be executed. 
		-- Add a new column with the name of the next table to be attached
	SET @sqlinsert = 'ALTER TABLE [oltplifestaging].[dbo].[observations] ADD' + QUOTENAME(@tablename) + 'NVARCHAR(150)'  
	EXEC (@sqlinsert)
	--SET @sqlinsert = 'INSERT INTO [oltplifestaging].[dbo].[observations] (' +  QUOTENAME(@tablename) + ') SELECT value FROM [oltplifesources].[dbo].' + QUOTENAME(@tablename)
	-- Update the newly created column instead of 'insert', as 'insert' adds new rows in each loop making table not eligible. 
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

-- Now the same for dim - whole table

-- Now merge dbo with dim

-- https://www.sqlshack.com/how-to-update-from-a-select-statement-in-sql-server/


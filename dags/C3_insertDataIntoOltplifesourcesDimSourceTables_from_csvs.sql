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

 
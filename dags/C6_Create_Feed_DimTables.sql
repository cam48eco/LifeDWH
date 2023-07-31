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
 
  
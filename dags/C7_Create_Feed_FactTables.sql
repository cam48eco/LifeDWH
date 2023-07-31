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

 






USE [EmptyDb]
Go

----------------------------------------------------
-- Create the assembly. When dropping the old one we must drop any
-- triggers created from it first.
----------------------------------------------------
DECLARE @ProjectPath NVARCHAR(1000)
SET @ProjectPath = '\\bsl2\bsl\doc\IanPerrin\Visual Studio 2005\Projects\BanksSadler.SqlServer'
DECLARE @SolutionFolder NVARCHAR (100)
SET @SolutionFolder = 'Triggers'
DECLARE @ProjectFolder NVARCHAR (100)
SET @ProjectFolder = 'DataAudit'
DECLARE @AssemblyPath NVARCHAR(1000)
SET @AssemblyPath = @ProjectPath + '\' + @SolutionFolder + '\' + @ProjectFolder + '\bin\Debug'
DECLARE @AssemblyName NVARCHAR(100)
SET @AssemblyName = 'DataAudit'
DECLARE @AssemblyObject NVARCHAR(1000)
SET @AssemblyObject = @AssemblyPath + '\' + @AssemblyName + '.dll'
DECLARE @AssemblyDebug NVARCHAR(1000)
SET @AssemblyDebug = @AssemblyPath + '\' + @AssemblyName + '.pdb'

if object_id('Audit_Address','TA') is not null drop trigger Audit_ADDRESS
if exists(select name from sys.assemblies where name = @AssemblyName)
	drop assembly [DataAudit]
create assembly [DataAudit] from @AssemblyObject with permission_set = safe
begin try
    -- This adds debugging info; the file will not be present in your "release" version
    -- (as opposed to your "debug" version), so we don't want to fail if it's not there.
    alter assembly [DataAudit] add file from @AssemblyDebug
end try
begin catch
end catch
GO



--------------------------
-- Create a test table.
--------------------------
if object_id('ADDRESS','U') is not null drop table ADDRESS
GO
create table ADDRESS (
	AddressId bigint IDENTITY(1,1) NOT NULL,
	AddressLine NVARCHAR(50) NULL,
	City NVARCHAR(50) NULL,
	StateProvince NVARCHAR(50) NULL,
	PostalCode NVARCHAR(10) NULL,
	CountryCode NVARCHAR(10) NULL,

	TempID INT NOT NULL DEFAULT 1
	
	constraint ADDRESS_PK primary key clustered 
	( AddressId ASC, TempID ASC ) with (IGNORE_DUP_KEY = off) on [PRIMARY]
) ON [PRIMARY]
GO

-- Populate the test table.
DECLARE @Counter int
SET @Counter = 0
DECLARE @CounterString NVARCHAR(10)
while @Counter < 10
begin
    SET @Counter = @Counter + 1
    SET @CounterString = cast(@Counter as NVARCHAR(10))
    DECLARE @AddressLine NVARCHAR(50)
    SET @AddressLine = 'Address Line ' + @CounterString
    DECLARE @City NVARCHAR(50)
    SET @City = 'City ' + @CounterString
    DECLARE @StateProvince NVARCHAR(50)
    SET @StateProvince = 'State/Province ' + @CounterString
    DECLARE @PostalCode NVARCHAR(10)
    SET @PostalCode = @CounterString
    DECLARE @CountryCode NVARCHAR(10)
    SET @CountryCode = 'C' + @CounterString

    insert into ADDRESS
    (
		AddressLine,
		City,
		StateProvince,
		PostalCode,
		CountryCode
    )
    values
    (
		@AddressLine,
		@City,
		@StateProvince,
		@PostalCode,
		@CountryCode
    )
end
GO


----------------------------------------------------
-- Associate the generic CLR trigger with the ADDRESS table.
----------------------------------------------------
CREATE TRIGGER Audit_ADDRESS
ON [ADDRESS] FOR INSERT, UPDATE, DELETE
AS EXTERNAL NAME [DataAudit].[Triggers].DataAudit
GO

 


----------------------------------------------------
-- Test Scripts
----------------------------------------------------
TRUNCATE TABLE [DataAudit]
-- Test "update"
update ADDRESS SET City = 'New City 4' where AddressID = 4

-- Test "insert"
insert into ADDRESS
(
    AddressLine,
    City,
    StateProvince,
    PostalCode,
    CountryCode
)
values
(
    'Inserted Address 1',
    'Inserted City 1',
    'Inserted StateProvince 1',
    '10001',
    'CI1'
)

update ADDRESS SET TempId = '3' where AddressID = 4

update ADDRESS SET [PostalCode] = null where AddressID = 4

update ADDRESS SET [PostalCode] = 'My Code', TempId = '4' where AddressID = 4

-- Test "delete"
delete from address where AddressID = 8


-- Show Data Audit
SELECT * FROM [Address]
SELECT * FROM [DataAudit]
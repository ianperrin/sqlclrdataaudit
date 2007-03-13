------------------------------------------
-- Configure the server for CLR operation.
------------------------------------------
sp_configure 'clr enabled', 1
GO
RECONFIGURE
GO
 

--------------------------
-- Create the audit table.
--------------------------
IF OBJECT_ID('DataAudit','U') IS NOT NULL DROP TABLE DataAudit
GO
CREATE TABLE DataAudit (
	-- audit key
	AuditId BIGINT IDENTITY(1,1) NOT NULL,

	-- required info for all auditing operations
	TableName SYSNAME NOT NULL,
	RowId XML NOT NULL,
	Operation NVARCHAR(10) NOT NULL,
	OccurredAt DATETIME NOT NULL,
	PerformedBy NVARCHAR(50) NOT NULL,

	-- the following fields are used only when Operation = 'UPDATE' 
	FieldName SYSNAME NULL,
	OldValue NVARCHAR(MAX) NULL,
	NewValue NVARCHAR(MAX) NULL,

	constraint AUDIT_PK primary key clustered 
	( AuditId asc ) with (IGNORE_DUP_KEY = off) on [PRIMARY]
) on [PRIMARY]
GO







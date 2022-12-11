CREATE OR ALTER PROCEDURE SetUpHashTree
AS
BEGIN
	
	SET NOCOUNT ON
	DECLARE @tables TABLE ([schema] VARCHAR(128), [table] VARCHAR(128))
	DECLARE @pks TABLE ([schema] VARCHAR(128), [table] VARCHAR(128), [column] VARCHAR(128), [datatype] VARCHAR(128), [len] INT, [position] INT)
	DECLARE @columns TABLE ([schema] VARCHAR(128), [table] VARCHAR(128), [column] VARCHAR(128), [datatype] VARCHAR(128), [position] INT)
	DECLARE @sql NVARCHAR(MAX)
	DECLARE @sql_2 NVARCHAR(MAX)
	DECLARE @sql_3 NVARCHAR(MAX)

	INSERT INTO @tables
	SELECT
		tables.TABLE_SCHEMA as [schema],
		tables.TABLE_NAME as [table]
	FROM INFORMATION_SCHEMA.TABLES tables
	WHERE tables.TABLE_TYPE = 'BASE TABLE' 
	AND tables.TABLE_SCHEMA NOT LIKE 'SqlHash%'
	ORDER BY [schema], [table]


	INSERT INTO @columns
	SELECT
		c.TABLE_SCHEMA [schema],
		c.TABLE_NAME [table],
		c.COLUMN_NAME [column],
		c.DATA_TYPE [dataType],
		row_number() over(PARTITION BY c.TABLE_SCHEMA, c.TABLE_NAME order by c.ORDINAL_POSITION) as [position]
		FROM
			INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN
			(SELECT 1 as [isComputed], c.[definition], s.name as [schema], o.name as [table], c.[name] as [column]
			FROM sys.computed_columns c
			INNER JOIN sys.objects o ON o.object_id = c.object_id
			INNER JOIN sys.schemas s ON s.schema_id = o.schema_id) computed
			ON c.TABLE_SCHEMA = computed.[schema] and c.TABLE_NAME = computed.[table] and c.COLUMN_NAME = computed.[column]
		LEFT JOIN
			(SELECT c.generated_always_type, s.name as [schema], o.name as [table], c.[name] as [column]
			FROM sys.columns c
			INNER JOIN sys.objects o ON o.object_id = c.object_id
			INNER JOIN sys.schemas s ON s.schema_id = o.schema_id) computed2
			ON c.TABLE_SCHEMA = computed2.[schema] and c.TABLE_NAME = computed2.[table] and c.COLUMN_NAME = computed2.[column]
		WHERE 
			c.TABLE_SCHEMA NOT LIKE 'SqlHash%'
		ORDER BY
			c.TABLE_SCHEMA, c.TABLE_NAME, [position]

	INSERT INTO @pks
	SELECT
			t.TABLE_SCHEMA [schema],
			t.TABLE_NAME [table],
			c.COLUMN_NAME [column],
			c.DATA_TYPE [dataType],
			c.CHARACTER_MAXIMUM_LENGTH [len],
			row_number() over(PARTITION BY c.TABLE_SCHEMA, c.TABLE_NAME order by c.ORDINAL_POSITION) as [position]
		FROM
		INFORMATION_SCHEMA.COLUMNS c
		INNER JOIN 	INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cc ON c.COLUMN_NAME = cc.COLUMN_NAME AND c.TABLE_NAME = cc.TABLE_NAME AND c.TABLE_SCHEMA = cc.TABLE_SCHEMA
		INNER JOIN  INFORMATION_SCHEMA.TABLE_CONSTRAINTS t ON t.TABLE_NAME = cc.TABLE_NAME AND t.TABLE_SCHEMA = cc.TABLE_SCHEMA AND t.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
		WHERE
			t.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY
			c.TABLE_SCHEMA, c.TABLE_NAME, [position]
	
	IF NOT EXISTS ( SELECT  *
				FROM    sys.schemas
				WHERE   name = N'SqlHash' )
	BEGIN
		SET @sql = 'CREATE SCHEMA SqlHash'
		EXEC (@sql)
	END

	DECLARE @schema VARCHAR(128)
	DECLARE @table VARCHAR(128)
	DECLARE @keys VARCHAR(MAX)
	DECLARE @keys_start VARCHAR(MAX)
	DECLARE @keys_end VARCHAR(MAX)
	DECLARE @keys2_start VARCHAR(MAX)
	DECLARE @keys2_end VARCHAR(MAX)
	DECLARE @keys3_start VARCHAR(MAX)
	DECLARE @keys3_end VARCHAR(MAX)
	DECLARE @join VARCHAR(MAX)

	DECLARE tablesCursor CURSOR  
	FOR SELECT DISTINCT [schema], [table] FROM @tables
	OPEN tablesCursor  
	FETCH NEXT FROM tablesCursor INTO @schema, @table
	
	WHILE @@FETCH_STATUS = 0  
	BEGIN  

			
		SET @keys = ''
		SELECT @keys = @keys + p.[column] + ','
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position
			
		SET @join = ''
		SELECT @join = @join + 'start_' + CONVERT(varchar, p.position) + '=' + p.[column] + ' AND '
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position
		
		SET @join = SUBSTRING(@join, 0, LEN(@join) - 3) 

		SET @keys3_start = ''
		SET @keys3_end = ''

		SELECT @keys3_start = @keys3_start + 'MIN([start_' +  CONVERT(varchar,  p.position) +  ']),'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position

		SELECT @keys3_end = @keys3_end + 'MAX([end_' + CONVERT(varchar,  p.position) +  ']),'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position

		SET @keys_start = ''
		SET @keys2_start = ''
		SELECT @keys_start = @keys_start + 'start_' +  CONVERT(varchar, p.position) + ' ' + CASE 
			WHEN p.[len] IS NULL OR p.datatype = 'hierarchyid' THEN p.datatype +  ','
			ELSE p.datatype + '(' + CONVERT(VARCHAR, p.[len]) + '),'
		END,
		 @keys2_start = @keys2_start + 'start_' +  CONVERT(varchar, p.position) + ','
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER By p.position
		SET @keys_start = SUBSTRING(@keys_start, 0, LEN(@keys_start))

		SET @keys_end = ''
		SET @keys2_end = ''
		SELECT @keys_end = @keys_end + 'end_' + CONVERT(varchar, p.position) + ' ' + CASE 
			WHEN p.[len] IS NULL OR p.datatype = 'hierarchyid' THEN p.datatype +  ','
			ELSE p.datatype + '(' + CONVERT(VARCHAR, p.[len]) + '),'
		END,
		 @keys2_end = @keys2_end + 'end_' +  CONVERT(varchar, p.position) + ','
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER By p.position

		SET @keys_end = SUBSTRING(@keys_end, 0, LEN(@keys_end))

		IF OBJECT_ID(N'SqlHash.' + @schema + '_' + @table, N'U') IS NOT NULL  
		BEGIN
			SET @sql = 'DROP TABLE SqlHash.' + @schema + '_' + @table
			EXEC (@sql)
		END

		SET @sql = 'CREATE TABLE SqlHash.' + @schema + '_' + @table + ' ([sqlhash_nodeId] int primary key identity(1,1), seq int, ' + @keys_start +  ',' + @keys_end + ', [sha2_256] varbinary(32), [parent] int, [level] int, [updated] datetime)'
		EXEC (@sql)

		SET @sql = 'CREATE UNIQUE INDEX [Index_U] ON SqlHash.' + @schema + '_' + @table + ' (level ASC, seq ASC)'
		EXEC (@sql)
		
		DECLARE @select VARCHAR(MAX)
		DECLARE @hash VARCHAR(MAX)

		SET @hash = 'CONCAT('
		SELECT @hash = @hash + CASE 
			WHEN p.datatype = 'xml' or p.datatype = 'geography' or p.datatype = 'hierarchyid' THEN 'CONVERT(nvarchar(max), [' + p.[column] + ']),'
			ELSE '[' + p.[column] + '],'
		END
		FROM @columns p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER By p.position
		SET @hash = SUBSTRING(@hash, 0, LEN(@hash)) + ')'


		SET @sql = 'INSERT INTO  [SqlHash].' + @schema + '_' + @table  +' SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),' + @keys + @keys + ' HASHBYTES(''SHA2_256'', ' + @hash + '), NULL, 0, NULL FROM ' + @schema + '.' + @table
		EXEC (@sql)

		SET @sql = '
			DECLARE @count INT
			DECLARE @level INT
			DECLARE @max INT
			DECLARE @keys_start VARCHAR(MAX)
			DECLARE @keys_end VARCHAR(MAX)

			SELECT @count = COUNT(*) FROM SqlHash.' + @schema + '_' + @table + '
			SET @max = CEILING(LOG(@count, 2)) + 1
			DECLARE @pks TABLE ([schema] VARCHAR(128), [table] VARCHAR(128), [column] VARCHAR(128), [datatype] VARCHAR(128), [len] INT, [position] INT)

			SET @level = 0
			WHILE (@level < @max)
			BEGIN
			
			
				INSERT [SqlHash].' + @schema + '_' + @table + '(seq, ' + @keys2_start +  @keys2_end + ' sha2_256, parent, [level], [updated])
				SELECT
					ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),' + @keys3_start + @keys3_end + ' HASHBYTES(''SHA2_256'',  STRING_AGG(CONVERT(varchar(max), sha2_256, 2), ''|'')) as [hash],
					NULL,
					@level + 1 as [level],
					NULL
				FROM [SqlHash].' + @schema + '_' + @table + ' p
				WHERE p.[level] = @level
				GROUP BY  p.[sqlhash_nodeId] / 2
	
				UPDATE t
					SET parent = t2.sqlhash_nodeId
				FROM 
					SqlHash.' + @schema + '_' + @table + ' t 
					INNER JOIN [SqlHash].' + @schema + '_' + @table + ' t2 ON CEILING(CONVERT(float, t.seq) / 2) = t2.seq AND t2.level = @level + 1
				WHERE 
					t.[level] = @level
	
					SET @level = @level + 1
			END
		'

		SET @sql_2 = '
CREATE OR ALTER TRIGGER ' + @schema + '.' + @table + '_SqlHash_Update
	ON ' + @schema + '.' + @table + '
	AFTER UPDATE
AS
BEGIN
	UPDATE p
		SET p.sha2_256 = HASHBYTES(''SHA2_256'', ' + @hash + '), updated = GETDATE()
	FROM SqlHash.' + @schema + '_' + @table + ' p
	INNER JOIN inserted ON ' + @join + '
END'


		SET @sql_3 = 'CREATE OR ALTER TRIGGER SqlHash.' + @schema + '_' +  @table + '_Update
	ON SqlHash.' + @schema + '_' + @table + '
	AFTER UPDATE
AS
BEGIN

SET NOCOUNT ON

DECLARE @level  INT = 0
DECLARE @parents TABLE (parent INT)
DECLARE @hashes TABLE (parent INT, [hash] VARBINARY(32))

INSERT INTO @parents
SELECT p.parent 
FROM SqlHash.' + @schema + '_' + @table + ' p
WHERE p.[level] = 0 AND p.sqlhash_nodeId IN (SELECT sqlhash_nodeId FROM inserted)

WHILE (1=1)
BEGIN
	INSERT INTO @hashes
	SELECT p.parent, HASHBYTES(''SHA2_256'',  STRING_AGG(CONVERT(varchar(max), p.sha2_256, 2), ''|''))
	FROM SqlHash.' + @schema + '_' + @table + ' p
	WHERE (p.[level] = @level) AND p.parent IN (SELECT parent FROM @parents)
	GROUP BY p.parent

	IF @@ROWCOUNT = 0
		BREAK

	UPDATE p
		SET sha2_256 = h.[hash], [updated] = GETDATE()
	FROM SqlHash.Production_Product p
	INNER JOIN @hashes h ON p.sqlhash_nodeId = h.parent
	

	INSERT INTO @parents
	SELECT p.parent 
	FROM SqlHash.Production_Product p
	WHERE p.sqlhash_nodeId IN (SELECT parent FROM @parents)

	SET @level = @level + 1
END
END

		'

		PRINT 'Setting up: ' + @schema + '.' + @table
		
		EXEC (@sql)
		EXEC (@sql_2)
		EXEC (@sql_3)

		PRINT 'Done'
		
		FETCH NEXT FROM tablesCursor INTO @schema, @table
	END
	
	CLOSE tablesCursor
	DEALLOCATE tablesCursor

END
	
GO

EXEC SetUpHashTree
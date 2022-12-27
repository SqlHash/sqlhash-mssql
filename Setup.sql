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
	DECLARE @sql_4 NVARCHAR(MAX)

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
	DECLARE @keys4_start VARCHAR(MAX)
	DECLARE @keys4_end VARCHAR(MAX)
	DECLARE @keys5_start VARCHAR(MAX)
	DECLARE @keys5_end VARCHAR(MAX)
	DECLARE @keys6_start VARCHAR(MAX)
	DECLARE @keys6_end VARCHAR(MAX)

	DECLARE @join VARCHAR(MAX)
	DECLARE @where VARCHAR(MAX)
	DECLARE @order VARCHAR(MAX)

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
		SET @keys = SUBSTRING(@keys, 0, LEN(@keys)) 

		SET @keys6_start = ''
		SELECT @keys6_start = @keys6_start + 'MIN(' + p.[column] + '),'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position
		SET @keys6_start = SUBSTRING(@keys6_start, 0, LEN(@keys6_start)) 

		SET @keys6_end = ''
		SELECT @keys6_end = @keys6_end + 'MAX(' + p.[column] + '),'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position
		SET @keys6_end = SUBSTRING(@keys6_end, 0, LEN(@keys6_end)) 

		SET @join = ''
		SELECT @join = @join + 'start_' + CONVERT(varchar, p.position) + '=' + p.[column] + ' AND '
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position
		SET @join = SUBSTRING(@join, 0, LEN(@join) - 3) 

		
		SET @order = ''
		SELECT @order = @order + 'p.start_' + CONVERT(varchar, p.position) + ' ASC,'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position
		SET @order = SUBSTRING(@order, 0, LEN(@order)) 

		SET @where = ''
		SELECT @where = @where + '@key' + CONVERT(varchar, p.position) + '=' + p.[column] + ' AND '
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position
		SET @where = SUBSTRING(@where, 0, LEN(@where) - 3) 
		
		SET @keys3_start = ''
		SET @keys3_end = ''

		SELECT @keys3_start = @keys3_start + 'MIN(p.[start_' +  CONVERT(varchar,  p.position) +  ']),'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position

		SELECT @keys3_end = @keys3_end + 'MAX(p.[end_' + CONVERT(varchar,  p.position) +  ']),'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position

		SET @keys4_start = ''
		SET @keys4_end = ''

		SELECT @keys4_start = @keys4_start + 'MIN([start_' +  CONVERT(varchar,  p.position) +  ']) as [start_' + CONVERT(varchar,  p.position) +'],'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position

		SELECT @keys4_end = @keys4_end +'MAX([end_' +  CONVERT(varchar,  p.position) +  ']) as [end_' + CONVERT(varchar,  p.position) +'],'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position

		SET @keys5_start = ''
		SET @keys5_end = ''

		SELECT @keys5_start = @keys5_start + 'start_' + CONVERT(varchar,  p.position) + '= t.[start_' + CONVERT(varchar,  p.position) +'],'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position

		SELECT @keys5_end = @keys5_end + 'end_' + CONVERT(varchar,  p.position) + '= t.[end_' + CONVERT(varchar,  p.position) +'],'
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
		SET @sql = 'CREATE TABLE SqlHash.' + @schema + '_' + @table + ' ([sqlhash_nodeId] int primary key identity(1,1), seq int, ' + @keys_start +  ',' + @keys_end + ', [sha2_256] varbinary(32), [parent] int,  [level] int, [updated] datetime, [order] int, [tmp] int NULL, [tmp2] int)'
		EXEC (@sql)

		SET @sql = 'CREATE INDEX [Index_P] ON SqlHash.' + @schema + '_' + @table + ' ([parent] ASC)'
		EXEC (@sql)
		SET @sql = 'CREATE INDEX [Index_L] ON SqlHash.' + @schema + '_' + @table + ' ([level] ASC)'
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


		SET @sql = 'INSERT INTO  [SqlHash].' + @schema + '_' + @table  +' SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),' + @keys + ',' + @keys + ', HASHBYTES(''SHA2_256'', ' + @hash + '), NULL, 0, NULL, 0, NULL, NULL FROM ' + @schema + '.' + @table
		EXEC (@sql)

		SET @sql = '
			DECLARE @count INT
			DECLARE @level INT
			DECLARE @max INT
			DECLARE @keys_start VARCHAR(MAX)
			DECLARE @keys_end VARCHAR(MAX)
			
		
			SELECT @count = COUNT(*) FROM SqlHash.' + @schema + '_' + @table + '
			SET @max = CEILING(LOG(@count + 1, 4)) + 1

			SET @level = 0
			WHILE (@level < @max)
			BEGIN

				CREATE TABLE #ids1 (id int identity(1,1) primary key, nodeId int)
				CREATE TABLE #ids2 (id int identity(1,1) primary key, nodeId int )

				INSERT INTO #ids1(nodeId)
				SELECT sqlhash_nodeId
				FROM [SqlHash].' + @schema + '_' + @table + ' p
				WHERE p.[level] = @level
				ORDER BY sqlhash_nodeId

				INSERT [SqlHash].' + @schema + '_' + @table + '(seq, ' + @keys2_start +  @keys2_end + ' sha2_256, parent, [level], [updated], [order])
				OUTPUT INSERTED.sqlhash_nodeId INTO #ids2(nodeId)
				SELECT
					ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),' + @keys3_start + @keys3_end + ' HASHBYTES(''SHA2_256'',  STRING_AGG(CONVERT(varchar(max), sha2_256, 2), ''|'') WITHIN GROUP ( ORDER BY ' + @order + ') ) as [hash],
					NULL,
					@level + 1 as [level],
					NULL,
					COUNT(*)
				FROM [SqlHash].' + @schema + '_' + @table + ' p
				INNER JOIN #ids1 i ON i.nodeId  = p.sqlhash_nodeId
				WHERE p.[level] = @level
				GROUP BY CEILING(CONVERT(float, i.id) / 4)

				UPDATE t
					SET parent = t2.sqlhash_nodeId
				FROM SqlHash.' + @schema + '_' + @table + ' t 
				INNER JOIN #ids1 i ON i.nodeId = t.sqlhash_nodeId
				INNER JOIN #ids2 i2 ON i2.id = CEILING(CONVERT(float, i.id) / 4)
				INNER JOIN SqlHash.' + @schema + '_' + @table + ' t2 ON i2.nodeId = t2.sqlhash_nodeId
				WHERE t.[level] = @level

				SET @level = @level + 1
				
				DROP TABLE #ids1
				DROP TABLE #ids2
			END

		'

		SET @sql_2 = '
CREATE OR ALTER TRIGGER ' + @schema + '.' + @table + '_SqlHash_Update
	ON ' + @schema + '.' + @table + '
	AFTER UPDATE
AS
BEGIN
	SET NOCOUNT ON

DECLARE @level  INT = 0
DECLARE @parents TABLE (parent INT)
DECLARE @hashes TABLE (parent INT, [hash] VARBINARY(32))

UPDATE p
	SET p.sha2_256 = HASHBYTES(''SHA2_256'', ' + @hash + '), updated = GETDATE()
FROM SqlHash.' + @schema + '_' + @table + ' p
INNER JOIN inserted ON ' + @join + '

INSERT INTO @parents
SELECT p.parent 
FROM SqlHash.' + @schema + '_' + @table + ' p
INNER JOIN inserted ON ' + @join + '
WHERE p.[level] = 0

WHILE (1=1)
BEGIN
	INSERT INTO @hashes
	SELECT p.parent, HASHBYTES(''SHA2_256'',  STRING_AGG(CONVERT(varchar(max), p.sha2_256, 2), ''|'') WITHIN GROUP ( ORDER BY ' + @order + ') )
	FROM SqlHash.' + @schema + '_' + @table + ' p
	WHERE (p.[level] = @level) AND p.parent IN (SELECT parent FROM @parents)
	GROUP BY p.parent

	IF @@ROWCOUNT = 0
		BREAK

	UPDATE p
		SET sha2_256 = h.[hash], [updated] = GETDATE()
	FROM SqlHash.' + @schema + '_' + @table + ' p
	INNER JOIN @hashes h ON p.sqlhash_nodeId = h.parent
	
	INSERT INTO @parents
	SELECT p.parent 
	FROM SqlHash.' + @schema + '_' + @table + ' p
	WHERE p.sqlhash_nodeId IN (SELECT parent FROM @parents)
	SET @level = @level + 1
END
END'

		DECLARE @vars VARCHAR(MAX)
		DECLARE @vars2 VARCHAR(MAX)

		SET @vars2 = ''
		SELECT @vars2 = @vars2 + '@key' + CONVERT(varchar, p.position) + ','
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		SET @vars2 = SUBSTRING(@vars2, 0, LEN(@vars2)) 

		SET @vars = ''
		SELECT @vars = @vars + 'DECLARE @key' + CONVERT(varchar, p.position) + ' ' + p.datatype + '
		'
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		ORDER BY p.position

		DECLARE @cond VARCHAR(MAX)
		DECLARE @cond2 VARCHAR(MAX)
		DECLARE @cond3 VARCHAR(MAX)

		SET @cond = '('
		SELECT @cond = @cond + 'start_' + CONVERT(varchar, p.position) + '<=' + '@key' + CONVERT(varchar, p.position) + ' AND end_' + CONVERT(varchar, p.position) + '>=' + '@key' + CONVERT(varchar, p.position) + ' AND '
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		SET @cond = @cond + ' 1 = 1 )'

		SET @cond2 = '('
		SELECT @cond2 = @cond2 + 'start_' + CONVERT(varchar, p.position) + '>' + '@key' + CONVERT(varchar, p.position) + ' AND '
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		SET @cond2 = @cond2 + ' 1 = 1 )'

		SET @cond3 = '('
		SELECT @cond3 = @cond3 + 'end_' + CONVERT(varchar, p.position) + '<' + '@key' + CONVERT(varchar, p.position) + ' AND '
		FROM @pks p
		WHERE p.[table] = @table AND p.[schema] = @schema
		SET @cond3 = @cond3 + ' 1 = 1 )'

		SET @sql_4 = 'CREATE OR ALTER TRIGGER [' + @schema + '].['+ @table + '_SqlHash_Delete]
	ON [' + @schema + '].[' + @table + ']
	AFTER DELETE
AS
BEGIN
	SET NOCOUNT ON

	' + @vars + '

	DECLARE @maxLevel INT
	DECLARE @nodeId INT
	DECLARE @i INT
	DECLARE @startNode INT
	DECLARE @foundNode INT
	DECLARE @order INT


	SELECT @maxLevel = MAX([level])
	FROM [SqlHash].' + @schema + '_' + @table + '
	
	SELECT @startNode = sqlhash_nodeId
	FROM [SqlHash].' + @schema + '_' + @table + '
	WHERE [level] = @maxLevel

	DECLARE cur_deleted CURSOR 
		FOR SELECT DISTINCT ' + @keys + ' FROM deleted
	OPEN cur_deleted  
	FETCH NEXT FROM cur_deleted INTO ' + @vars2 + '
	
	WHILE @@FETCH_STATUS = 0  
	BEGIN
		DECLARE @path TABLE (nodeId INT)

		SET @nodeId = @startNode

		WHILE (1=1)
		BEGIN
			INSERT INTO @path VALUES(@nodeId)
			SET @foundNode = NULL

			SELECT TOP 1  @foundNode = t.sqlhash_nodeId
			FROM  [SqlHash].' + @schema + '_' + @table + ' t
			WHERE parent = @nodeId AND (' + @cond + ')
			ORDER BY seq ASC
	
			IF @foundNode IS NOT NULL
			BEGIN
				SET @nodeId = @foundNode
			END
			ELSE
			BEGIN
				BREAK
			END
		END

		-- update the tree (tree could be unbalanced)
		SELECT 
					p.parent,
					' + @keys4_start + '
					' + @keys4_end + '
					HASHBYTES(''SHA2_256'',  STRING_AGG(CONVERT(varchar(max), sha2_256, 2), ''|'') WITHIN GROUP ( ORDER BY ' + @order + ') ) as [hash],
					COUNT(*) as [count]
		INTO #tmp
		FROM [SqlHash].' + @schema + '_' + @table + ' p
		WHERE p.parent IN (SELECT nodeId from @path)
		GROUP BY p.parent

		UPDATE p
		 SET
			sha2_256 = t.[hash],
			' + @keys5_start + '
			' + @keys5_end + '
			updated = GETDATE(),
			[order] = t.[count]
		FROM [SqlHash].' + @schema + '_' + @table + ' p
		INNER JOIN #tmp t ON p.sqlhash_nodeId = t.parent

		DROP TABLE #tmp
		FETCH NEXT FROM cur_deleted INTO ' + @vars2 + '

		DELETE FROM [SqlHash].' + @schema + '_' + @table + '
		WHERE sqlhash_nodeId IN (SELECT nodeId from @path) AND [order] = 0

	END

	CLOSE cur_deleted
	DEALLOCATE cur_deleted

END'

SET @sql_3 = 'CREATE OR ALTER TRIGGER [' + @schema + '].['+ @table + '_SqlHash_Insert]
	ON [' + @schema + '].[' + @table + ']
	AFTER INSERT
AS
BEGIN
	SET NOCOUNT ON

	' + @vars + '

	DECLARE @maxLevel INT
	DECLARE @nodeId INT
	DECLARE @i INT
	DECLARE @startNode INT
	DECLARE @foundNode INT
	DECLARE @order INT

	SELECT @maxLevel = MAX([level])
	FROM [SqlHash].' + @schema + '_' + @table + '

	SELECT @startNode = sqlhash_nodeId
	FROM [SqlHash].' + @schema + '_' + @table + '
	WHERE level = @maxLevel AND level > 0

	DECLARE @toBalance TABLE (nodeId INT primary key)

	DECLARE cur_inserted CURSOR 
		FOR SELECT DISTINCT ' + @keys + ' FROM inserted
	OPEN cur_inserted  
	FETCH NEXT FROM cur_inserted INTO ' + @vars2 + '
	
	WHILE @@FETCH_STATUS = 0  
	BEGIN
		DECLARE @path TABLE (nodeId INT)

		SET @nodeId = @startNode

		WHILE (1=1)
		BEGIN
			INSERT INTO @path VALUES(@nodeId)

			SET @foundNode = NULL

			SELECT TOP 1  @foundNode = t.sqlhash_nodeId
			FROM  [SqlHash].' + @schema + '_' + @table + ' t
			WHERE [level] > 0 AND parent = @nodeId AND (' + @cond + ')
			ORDER BY seq ASC
	
			IF @foundNode IS NOT NULL
			BEGIN
				SET @nodeId = @foundNode
				CONTINUE
			END

			SELECT TOP 1  @foundNode = t.sqlhash_nodeId
			FROM  [SqlHash].' + @schema + '_' + @table + ' t
			WHERE [level] > 0 AND parent = @nodeId AND (' + @cond2 + ')
			ORDER BY seq ASC

			IF @foundNode IS NOT NULL
			BEGIN
				SET @nodeId = @foundNode
				CONTINUE
			END

			SELECT TOP 1  @foundNode = t.sqlhash_nodeId
			FROM  [SqlHash].' + @schema + '_' + @table + ' t
			WHERE [level] > 0 AND parent = @nodeId AND (' + @cond3 + ')
			ORDER BY seq DESC

			IF @foundNode IS NOT NULL
			BEGIN
				SET @nodeId = @foundNode
			END
			ELSE
			BEGIN
				BREAK
			END
			
		END

		IF (@nodeId IS NULL)
		BEGIN

			INSERT INTO  [SqlHash].' + @schema + '_' + @table  +'
			SELECT 0,' + @keys + ',' + @keys + ', HASHBYTES(''SHA2_256'', ' + @hash + '), NULL, 1, NULL, 1, NULL, NULL FROM inserted WHERE ' + @where +'

			INSERT INTO  [SqlHash].' + @schema + '_' + @table  +'
			SELECT 0,' + @keys + ',' + @keys + ', HASHBYTES(''SHA2_256'', ' + @hash + '), SCOPE_IDENTITY(), 0, NULL, 0, NULL, NULL FROM inserted WHERE ' + @where +'

		END
		ELSE
		BEGIN
			INSERT INTO  [SqlHash].' + @schema + '_' + @table  +'
				OUTPUT INSERTED.sqlhash_nodeId INTO @toBalance(nodeId)
			SELECT 0,' + @keys + ',' + @keys + ', HASHBYTES(''SHA2_256'', ' + @hash + '), @nodeId, 0, NULL, 0, NULL, NULL FROM inserted WHERE ' + @where +'

			UPDATE [SqlHash].' + @schema + '_' + @table + ' 
			SET [order] = [order] + 1
			WHERE sqlhash_nodeId = @nodeId

		END


		
		
		FETCH NEXT FROM cur_inserted INTO ' + @vars2 + '
	END
	CLOSE cur_inserted
	DEALLOCATE cur_inserted

	DECLARE @lev INT 
	DECLARE @count INT

	SET @lev = 0
	

	WHILE (1=1)
	BEGIN

		INSERT INTO @toBalance(nodeId)
		SELECT t.sqlhash_nodeId
		FROM [SqlHash].' + @schema + '_' + @table + ' t
		WHERE ((t.parent is null) OR (t.parent IN (SELECT parent FROM [SqlHash].' + @schema + '_' + @table + ' t2 WHERE t2.sqlhash_nodeId IN (SELECT nodeId FROM @toBalance))))
			AND t.sqlhash_nodeId NOT IN (SELECT nodeId FROM @toBalance)
			AND t.[level] = @lev

		CREATE TABLE #ids1 (id int identity(1,1) primary key, seq int, [create] bit, nodeId int, parentId int, [order] int)

		INSERT INTO #ids1(seq, [create], nodeId, parentId, [order])
		SELECT 
			x.seq, 
			CASE  
				WHEN [seq] > 4 or x.parent is NULL THEN 1
				ELSE 0
			END as [create],
			x.sqlhash_nodeId,
			x.parent, 
			x.[order]
		FROM (
		SELECT ROW_NUMBER() OVER(PARTITION BY p.parent ORDER BY ' + @order + ') as [seq], p.[sqlhash_nodeId], p.[parent], p2.[order], p.[level]
		FROM [SqlHash].' + @schema + '_' + @table + ' p
		LEFT JOIN [SqlHash].' + @schema + '_' + @table + ' p2 ON p.parent = p2.sqlhash_nodeId
		WHERE p.sqlhash_nodeId IN (SELECT nodeId FROM @toBalance)) x


		DELETE FROM @toBalance

		INSERT [SqlHash].' + @schema + '_' + @table + '(seq, ' + @keys2_start +  @keys2_end + ' sha2_256, parent, [level], [updated], [order], [tmp], [tmp2])
			OUTPUT inserted.sqlhash_nodeId INTO @toBalance  
		SELECT
			0,' + @keys3_start + @keys3_end + ' HASHBYTES(''SHA2_256'',  STRING_AGG(CONVERT(varchar(max), p.sha2_256, 2), ''|'')  WITHIN GROUP ( ORDER BY ' + @order + ') ) as [hash],
			NULL,
			@lev + 1 as [level],
			NULL,
			COUNT(*),
			ROW_NUMBER() OVER(ORDER BY (SELECT NULL)),
			 -1
		FROM [SqlHash].' + @schema + '_' + @table + ' p
		INNER JOIN #ids1 t ON p.sqlhash_nodeId = t.nodeId
		WHERE t.[create] = 1 
		GROUP BY t.parentId
		
		-- update nodes at level = @lev
		UPDATE t SET 
			parent = t2.sqlhash_nodeId,
			updated = GETDATE()
		FROM SqlHash.' + @schema + '_' + @table + ' t 
		INNER JOIN #ids1 i ON i.nodeId = t.sqlhash_nodeId
		INNER JOIN  SqlHash.' + @schema + '_' + @table + ' t2 ON  CEILING(CONVERT(float, i.id) / 4) = t2.tmp AND t2.level = @lev + 1 AND t2.tmp2 = -1
		WHERE t.[level] = @lev and i.[create] = 1

		UPDATE t SET 
			seq = i.seq,
			updated = GETDATE()
		FROM SqlHash.' + @schema + '_' + @table + ' t 
		INNER JOIN #ids1 i ON i.nodeId = t.sqlhash_nodeId
		WHERE t.[level] = @lev

		UPDATE t SET 
			[tmp2] = NULL
		FROM SqlHash.' + @schema + '_' + @table + ' t 
		WHERE t.[level] = @lev + 1

		UPDATE t2 SET 
			[order] = CASE 
						WHEN t2.[order] > 4 THEN 4
						ELSE t2.[order]
					END
		FROM SqlHash.' + @schema + '_' + @table + ' t 
		INNER JOIN SqlHash.' + @schema + '_' + @table + ' t2 ON t2.sqlhash_nodeId = t.parent
		WHERE t.level = @lev

		DROP TABLE #ids1

		SELECT @count = COUNT(*) FROM [SqlHash].' + @schema + '_' + @table + ' t WHERE t.level = @lev + 1

		IF (@count = 1 OR @count = 0)
			BREAK
	
		SET @lev = @lev + 1
	END
END'

		PRINT 'Setting up: ' + @schema + '.' + @table
		
		EXEC (@sql)
		EXEC (@sql_2)
		EXEC (@sql_3)
		EXEC (@sql_4)

		PRINT 'Done'
		
		FETCH NEXT FROM tablesCursor INTO @schema, @table
	END
	
	CLOSE tablesCursor
	DEALLOCATE tablesCursor

END
	
GO

EXEC SetUpHashTree
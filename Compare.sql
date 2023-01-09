DECLARE @level_1 INT, @level_2 INT

DECLARE @hash_1 VARBINARY(32), @hash_2 VARBINARY(32)

SELECT @level_1 = MAX([level]) FROM [AdventureWorks2019].[SqlHash].[dbo_ErrorLog]
SELECT @level_2 = MAX([level]) FROM [AdventureWorks2019].[SqlHash].[dbo_ErrorLog2]

SELECT @hash_1 = sha2_256
FROM [AdventureWorks2019].[SqlHash].[dbo_ErrorLog]
WHERE [level] = @level_1

SELECT @hash_2 = sha2_256
FROM [AdventureWorks2019].[SqlHash].[dbo_ErrorLog2]
WHERE [level] = @level_2

IF @hash_2 = @hash_1
BEGIN
	PRINT 'Roots same'
END
ELSE
BEGIN
	PRINT 'Roots different'
END


# sqlsizer-mssql-hash-tree
A general-purpose hash trees support for Microsoft SQL Server databases (in development)


# Idea for project

1. In order to maintain and create the hash tree for SQL Server database, the special tables to be created and mantained on MS SQL Server side:

![image](https://user-images.githubusercontent.com/115426/203154476-91a77bc3-0578-44fb-a5f4-d708b420bd6d.png)

2. The structures needs to be updated by triggers on SQL Server side

3. Initially, the structures needs to be filled using special stored procedure

4. The hash tree support will be deployed in a single SQL script.

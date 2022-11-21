# sqlsizer-mssql-hash-tree
A general-purpose hash trees support for Microsoft SQL Server databases (in development)


# Idea for project

1. In order to maintain and create the hash tree for SQL Server database, the special tables needs to be created and mantained on MS SQL Server side:

Example:

![image](https://user-images.githubusercontent.com/115426/203154476-91a77bc3-0578-44fb-a5f4-d708b420bd6d.png)


2. The structures will to be updated by triggers on SQL Server side

3. Initially, the structures needs to be filled using special stored procedure

4. The hash tree support will be deployed in a single SQL script.


# Roadmap/plan
1. Finish "Idea for project"
2. Write a T-SQL script to create hash tree node tables for all tables in MSSQL database
3. Write a T-SQL script to create hash tree for the specific table
4. Write triggers to update/refresh hash tree
5. Write stored procedure for comparing two tables using hash tree
6. Make logic more general to create hash tree for part of table rows
7. Make logic more general to create hash tree for subsets
8. Write stored procedure for comparing two databases / subsets


# License

The project will be published under MIT license.

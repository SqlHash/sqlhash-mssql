# sqlhash-mssql
A general-purpose hash trees support for Microsoft SQL Server databases (deployed as a single T-SQL script)

# Idea for project

1. In order to maintain and create the hash tree for SQL Server database, the special tables needs to be created and mantained:

Example:

![image](https://user-images.githubusercontent.com/115426/206576451-d35b7446-0966-4fc8-965a-eb70889c85a8.png)

2. The structures will to be updated by triggers on SQL Server side

3. Initially, the structures needs to be filled using special stored procedure

4. The hash tree support will be deployed in a single T-SQL script.


# Roadmap/plan
1. Finish "Idea for project" and Roadmap (**done**)
2. Write a T-SQL script to create hash tree node tables for all tables in MSSQL database (**done**)
3. Write a T-SQL script to create hash tree for the specific table (**done**)

4. Write triggers to update/refresh hash tree: 
  - update (**done**)
  - delete (**done**)
  - insert (**partially done**)
5. Write stored procedure for comparing two tables using hash tree
6. Make logic more general to create hash tree for part of table rows
7. Make logic more general to create hash tree for subsets
8. Write stored procedure for comparing two databases / subsets
9. Add more features using hash trees


# License

The project is published under MIT license.

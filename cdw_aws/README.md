# Cloud Data Warehouse in a AWS Redshift instance

### Objective
To create a Data Warehouse, using an ETL pipeline that copies staging tables from an S3 instance and builds a star schema from them.
The database is called Sparkify and its star schema is optimized for performing analytical queries for artists, songs, users and activity monitoring.

### ERD for the Sparkify Database

![image](https://user-images.githubusercontent.com/15125406/219561311-09c96ab0-fb9a-42cf-8eeb-eda77d46d4a6.png)


### Project Structure

```
cdw_aws
 - dwh.cfg
 - sql_queries.py
 - create_tables.py
 - etl.py
```
#### dwh.cfg
Is a .cfg extension file that configures the access to a redshift instance by means of the usage of an IAM role (access/security logging), and a endpoint string (host).
The database, its name, user and password are also present in this file.

#### sql_queries.py
A python file that gathers all the queries string for every single analysis made to the Sparkify database. In it are queries for dropping, creating, copying from S3 instances, inserting, etc.

#### etl.py
A python file with a pipeline that creates functions to extract, transform and load information from staging tables to star-schema tables. It imports query strings from sql_queries to perform the ETL operations.

#### create-tables.py
A python file that presents creation functions for staging tables and star-schema tables. It imports query strings from sql_queries to perform the ETL operations.

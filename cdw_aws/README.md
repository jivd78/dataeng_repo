# Cloud Data Warehouse in a AWS Redshift instance

### Objective
To create a Data Warehouse, using an ETL pipeline that copies staging tables from an S3 instance and builds a star schema from them.
The database is called Sparkify and its star schema is optimized for performing analytical queries for artists, songs, users and activity monitoring.

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

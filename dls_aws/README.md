## Project Instructions

Using AWS Glue, AWS S3, Python, and Spark, create or generate Python scripts to build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.

## Requirements

### Starting Point:

To simulate the data coming from the various sources, you will need to create your own S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there as a starting point.

My particular bucket created at S3 is: ``` s3://stedi-jivd ```
The structure inside de bucket is as follows:

``` 
    s3://stedi-jivd
     |_customer
     |   |_landing
     |   |_trusted
     |   |_curated
     |_accelerometer
     |   |_landing
     |   |_trusted
     |   |_curated
     |_step_trainer
     |   |_landing
     |   |_trusted
     |   |_curated
     |_athena
     |_scripts
     |_sparkHistoryLogs    
```    

### Landing Zone

#### Use Glue Studio to ingest data from an S3 bucket

customer_landing_to_trusted.py and accelerometer_landing_to_trusted_zone.py Glue jobs have a node that connects to S3 bucket for customer and accelerometer landing zones.

#### Manually create a Glue Table using Glue Console from JSON data

SQL DDL scripts customerlanding.sql and accelerometer_landing.sql include all of the JSON fields in the data input files, and are appropriately typed (not everything is a string

#### Use Athena to query the Landing Zone.

Screenshot shows a select statement from Athena showing the customer landing data and accelerometer landing data, where the customer landing data has multiple rows where shareWithResearchAsOfDate is blank.

### Trusted Zone

#### Configure Glue Studio to dynamically update a Glue Table schema from JSON data

Glue Job Python code shows that the option to dynamically infer and update schema is enabled.

#### Use Athena to query Trusted Glue Tables

A screenshot that shows a select * statement from Athena showing the customer landing data, where the resulting customer trusted data has no rows where shareWithResearchAsOfDate is blank.

#### Join Privacy tables with Glue Jobs

Glue jobs have inner joins that join up with the customer_landing table on the serialnumber field. (customer_landing_to_trusted.py and accelerometer_landing_to_trusted_zone.py )

#### Filter protected PII with Spark in Glue Jobs

Glue jobs drop data that doesn’t have data in the sharedWithResearchAsOfDate column (customer_landing_to_trusted.py and accelerometer_landing_to_trusted_zone.py )

### Curated Zone

#### Write a Glue Job to join trusted data

Glue jobs do inner joins with the customer_trusted table. (Customer_trusted_to_curated.py and trainer_trusted_to_curated.py)

#### Write a Glue Job to create curated data

The curated data from the Glue tables is sanitized and only contains only customer data from customer records that agreed to share data, and is joined with the correct accelerometer data.

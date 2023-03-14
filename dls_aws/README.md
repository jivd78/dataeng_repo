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

Create scripts for customer_landing_to_trusted.py and accelerometer_landing_to_trusted_zone.py Glue jobs have a node that connects to S3 bucket for customer and accelerometer landing zones.

Please see the job scripts at `customer_landing_to_trusted.py` and `accelerometer_landing_to_trusted.py` 

#### Manually create a Glue Table using Glue Console from JSON data

SQL DDL scripts customerlanding.sql and accelerometer_landing.sql include all of the JSON fields in the data input files, and are appropriately typed (not everything is a string.

Please see the sql queries `customer_landing_table.sql` and `accelerometer_landing_table.sql`.

#### Use Athena to query the Landing Zone.

Screenshot shows a select statement from Athena showing the customer landing data and accelerometer landing data, where the customer landing data has multiple rows where shareWithResearchAsOfDate is blank.

##### customer_landing_table.png
![customer_landing_table](https://user-images.githubusercontent.com/15125406/224882830-1508bd1c-4961-4a81-abe8-6a1495929d1e.png)

##### accelerometer_landing_table.png
![accelerometer_landing_table](https://user-images.githubusercontent.com/15125406/224882864-81e79c09-e56d-414e-8ec2-d131e3687e5c.png)

### Trusted Zone

#### Configure Glue Studio to dynamically update a Glue Table schema from JSON data

Glue Job Python code shows that the option to dynamically infer and update schema is enabled.

The `glueContext.create_dynamic_frame.from_options()` is how spark infer the schema. It can be visualized in all the scripts.

#### Use Athena to query Trusted Glue Tables

A screenshot that shows a select * statement from Athena showing the customer landing data, where the resulting customer trusted data has no rows where shareWithResearchAsOfDate is blank.

##### Screenshot with customer_landing_table queried to show only trusted data: 
![image](https://user-images.githubusercontent.com/15125406/224886493-d9e015e1-6d90-482f-aa6d-9c436e7e108f.png)

#### Join Privacy tables with Glue Jobs

Glue jobs have inner joins that join up with the customer_landing table on the serialnumber field. (customer_landing_to_trusted.py and accelerometer_landing_to_trusted_zone.py )

Both `customer_landing_to_trusted.py` and `accelerometer_landing_to_trusted.py` left the serialnumber field to use for curated tables.

#### Filter protected PII with Spark in Glue Jobs

Glue jobs drop data that doesn’t have data in the sharedWithResearchAsOfDate column (customer_landing_to_trusted.py and accelerometer_landing_to_trusted_zone.py )

Both `customer_landing_to_trusted.py` and `accelerometer_landing_to_trusted.py` drop registers that do not have sharedWithResearchAsOfDate data, since it means they were not authorized for use.

### Curated Zone

#### Write a Glue Job to join trusted data

Glue jobs do inner joins with the customer_trusted table. (Customer_trusted_to_curated.py)

Please see the job scripts at `customer_trusted_to_curated.py`.

#### Write a Glue Job to create curated data

The curated data from the Glue tables is sanitized and only contains only customer data from customer records that agreed to share data, and is joined with the correct accelerometer data.

Please see the job scripts at `machine_learning_curated.py`.



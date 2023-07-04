# Automating Data Pipelines

## Prerequisites
### Prerequisites:

Create an IAM User in AWS.

Configure Redshift Serverless in AWS.

### Setting up Connections

Connect Airflow and AWS

Connect Airflow to AWS Redshift Serverless

### Orginal Datasets

There are two datasets. Here are the s3 links for each:

Log data: `s3://udacity-dend/log_data`

Song data: `s3://udacity-dend/song_data`

Copy the data to your own bucket.

### Copy S3 Data

The data is stored in a Udacity's S3 bucket. This bucket is in the US West AWS Region. To simplify things, we are going to copy the data to your own bucket, so Redshift can access the bucket.
Create your own S3 bucket using the AWS Cloudshell (this is just an example - buckets need to be unique across all AWS accounts): aws s3 mb s3://sean-murdock/

Copy the data from the udacity bucket to the home cloudshell directory: ```

`aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive`

`aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive`

### Configuring the DAG

In the DAG, add default parameters according to these guidelines

The DAG does not have dependencies on past runs
On failure, the task are retried 3 times
Retries happen every 5 minutes
Catchup is turned off
Do not email on retry
In addition, configure the task dependencies so that after the dependencies are set, the graph view follows the flow shown in the image below.

![image](https://github.com/jivd78/dataeng_repo/assets/15125406/9f18bd8f-d662-47bb-aa81-e908c8c80c49)

### The Database




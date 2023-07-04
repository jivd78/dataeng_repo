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

The data for the next few exercises is stored in Udacity's S3 bucket. This bucket is in the US West AWS Region. To simplify things, we are going to copy the data to your own bucket, so Redshift can access the bucket.
If you haven't already, create your own S3 bucket using the AWS Cloudshell (this is just an example - buckets need to be unique across all AWS accounts): aws s3 mb s3://sean-murdock/

Copy the data from the udacity bucket to the home cloudshell directory: ```

`aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive`
`aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive`

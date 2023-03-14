import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing_from_s3
accelerometer_landing_from_s3_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-jivd/step_trainer/landing"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_from_s3_node1",
)

# Script generated for node customer_trusted_table
customer_trusted_table_node1678501925600 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedidb",
        table_name="customer_curated",
        transformation_ctx="customer_trusted_table_node1678501925600",
    )
)

# Script generated for node Join
Join_node1678502015554 = Join.apply(
    frame1=accelerometer_landing_from_s3_node1,
    frame2=customer_trusted_table_node1678501925600,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1678502015554",
)

# Script generated for node Drop Fields
DropFields_node1678502125219 = DropFields.apply(
    frame=Join_node1678502015554,
    paths=[
        "user",
        "x",
        "y",
        "z",
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1678502125219",
)

# Script generated for node Amazon S3
AmazonS3_node1678502165619 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1678502125219,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-jivd/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1678502165619",
)

job.commit()

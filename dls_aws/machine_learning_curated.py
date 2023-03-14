import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_trusted_table
step_trainer_trusted_table_node1678749438055 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedidb",
        table_name="step_trainter_trusted",
        transformation_ctx="step_trainer_trusted_table_node1678749438055",
    )
)

# Script generated for node accelerometer_trusted_table
accelerometer_trusted_table_node1678501925600 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedidb",
        table_name="accelerometer_trusted",
        transformation_ctx="accelerometer_trusted_table_node1678501925600",
    )
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1678749888496 = ApplyMapping.apply(
    frame=step_trainer_trusted_table_node1678749438055,
    mappings=[
        ("sensorreadingtime", "long", "`(step_trainer) sensorreadingtime`", "long"),
        ("serialnumber", "string", "`(step_trainer) serialnumber`", "string"),
        ("distancefromobject", "int", "`(step_trainer) distancefromobject`", "int"),
        ("timestamp", "long", "`(step_trainer) timestamp`", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1678749888496",
)

# Script generated for node Join
Join_node1678502015554 = Join.apply(
    frame1=accelerometer_trusted_table_node1678501925600,
    frame2=RenamedkeysforJoin_node1678749888496,
    keys1=["timestamp"],
    keys2=["`(step_trainer) timestamp`"],
    transformation_ctx="Join_node1678502015554",
)

# Script generated for node Drop Fields
DropFields_node1678502125219 = DropFields.apply(
    frame=Join_node1678502015554,
    paths=[],
    transformation_ctx="DropFields_node1678502125219",
)

# Script generated for node Filter
Filter_node1678749948815 = Filter.apply(
    frame=DropFields_node1678502125219,
    f=lambda row: (
        not (row["timestamp"] == 0) and not (row["sharewithresearchasofdate"] == 0)
    ),
    transformation_ctx="Filter_node1678749948815",
)

# Script generated for node ML_curated
ML_curated_node1678502165619 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1678749948815,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-jivd/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="ML_curated_node1678502165619",
)

job.commit()

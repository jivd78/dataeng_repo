import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted_table
accelerometer_trusted_table_node1678594261455 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedidb",
        table_name="accelerometer_trusted",
        transformation_ctx="accelerometer_trusted_table_node1678594261455",
    )
)

# Script generated for node customer_trusted_table
customer_trusted_table_node1678501925600 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedidb",
        table_name="customer_trusted",
        transformation_ctx="customer_trusted_table_node1678501925600",
    )
)

# Script generated for node Join
Join_node1678502015554 = Join.apply(
    frame1=customer_trusted_table_node1678501925600,
    frame2=accelerometer_trusted_table_node1678594261455,
    keys1=["email", "sharewithresearchasofdate"],
    keys2=["user", "sharewithresearchasofdate"],
    transformation_ctx="Join_node1678502015554",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
where timestamp >= shareWithResearchAsOfDate
"""
SQLQuery_node1678806723215 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": Join_node1678502015554},
    transformation_ctx="SQLQuery_node1678806723215",
)

# Script generated for node Customer_curated_to_s3
Customer_curated_to_s3_node1678502165619 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1678806723215,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-jivd/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Customer_curated_to_s3_node1678502165619",
)

job.commit()

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


"""These scripts reads data from an S3 bucket in Parquet format and creates a DynamicFrame.
It connects to the specified S3 path and retrieves the data, which can then be processed or transformed as needed.
This script is part of an AWS Glue job that processes data from S3.
The data is expected to be in Parquet format, and the S3 path is specified in the connection options.
This script is designed to be run in an AWS Glue environment, where it can access the necessary resources and permissions to read from S3."""
# Script for node Amazon S3 --apartment_attributes
AmazonS3_node1750164168812 = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"paths": ["s3://project2dt/data/apartment_attributes/"], "recurse": True}, 
    transformation_ctx="AmazonS3_node1750164168812")

# Script for node Amazon S3 --bookings
AmazonS3_node1750165947022 = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", format="parquet", 
    connection_options={"paths": ["s3://project2dt/data/bookings/"], "recurse": True}, 
    transformation_ctx="AmazonS3_node1750165947022")


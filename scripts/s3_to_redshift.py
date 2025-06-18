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

# Script for node Amazon S3 --user_view
AmazonS3_node1750166033519 = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"paths": ["s3://project2dt/data/user_view/"], "recurse": True}, 
    transformation_ctx="AmazonS3_node1750166033519")

# Script for node Amazon S3 --apartments
AmazonS3_node1750165827666 = glueContext.create_dynamic_frame.from_options(
    format_options={}, 
    connection_type="s3", 
    format="parquet", 
    connection_options={"paths": ["s3://project2dt/data/apartments/"], "recurse": True}, 
    transformation_ctx="AmazonS3_node1750165827666")

"""The scripts below connects to an Amazon Redshift database and writes the 
data from the DynamicFrames created from S3.
It uses the GlueContext to write the data to the specified Redshift table, 
creating the table if it does not exist."""

# Script for node Amazon Redshift 
# --apartment_attributes to raw_data.apartment_attributes in Redshift
AmazonRedshift_node1750164179207 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1750164168812, connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "raw_data.apartment_attributes",
        "connectionName": "Redshift connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS raw_data.apartment_attributes "
        "("
        "id INTEGER,"
        " category VARCHAR, "
        "body VARCHAR, "
        "amenities VARCHAR, "
        "bathrooms INTEGER, "
        "bedrooms INTEGER, "
        "fee DECIMAL, "
        "has_photo VARCHAR, "
        "pets_allowed VARCHAR, "
        "price_display VARCHAR, "
        "price_type VARCHAR, "
        "square_feet INTEGER, "
        "address VARCHAR, "
        "cityname VARCHAR, "
        "state VARCHAR, "
        "latitude DECIMAL, "
        "longitude DECIMAL);"}, 
        transformation_ctx="AmazonRedshift_node1750164179207")

# Script for node Amazon Redshift
# --bookings to raw_data.bookings in Redshift
AmazonRedshift_node1750165954380 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1750165947022, connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/",
    "useConnectionProperties": "true", 
    "dbtable": "raw_data.bookings", 
    "connectionName": "Redshift connection", 
    "preactions": "CREATE TABLE IF NOT EXISTS raw_data.bookings "
    "("
    "booking_id INTEGER,"
    "user_id INTEGER,"
    "apartment_id INTEGER,"
    "booking_date VARCHAR,"
    "checkin_date VARCHAR,"
    "checkout_date VARCHAR,"
    "total_price DECIMAL,"
    "currency VARCHAR,"
    "booking_status VARCHAR);"}, 
    transformation_ctx="AmazonRedshift_node1750165954380")

# Script for node Amazon Redshift
AmazonRedshift_node1750166039347 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1750166033519, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "raw_data.user_viewing",
        "connectionName": "Redshift connection",
        "preactions": "CREATE TABLE IF NOT EXISTS raw_data.user_viewing "
        "("
        "user_id INTEGER, "
        "apartment_id INTEGER, "
        "viewed_at VARCHAR, "
        "is_wishlisted VARCHAR, "
        "call_to_action VARCHAR);"
    }, 
    transformation_ctx="AmazonRedshift_node1750166039347"
)
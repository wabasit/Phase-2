import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script for node Amazon Redshift
AmazonRedshift_node1750167535303 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
                        "useConnectionProperties": "true",
                        "dbtable": "raw_data.apartment_attributes", 
                        "connectionName": "Redshift connection"}, 
                        transformation_ctx="AmazonRedshift_node1750167535303")

# Script for node Amazon Redshift
AmazonRedshift_node1750169164676 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
                        "useConnectionProperties": "true", 
                        "dbtable": "raw_data.user_viewing", 
                        "connectionName": "Redshift connection"}, 
                        transformation_ctx="AmazonRedshift_node1750169164676")

# Script for node Amazon Redshift
AmazonRedshift_node1750169072593 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
                        "useConnectionProperties": "true", 
                        "dbtable": "raw_data.apartments", 
                        "connectionName": "Redshift connection"}, 
    transformation_ctx="AmazonRedshift_node1750169072593")

# Script for node Amazon Redshift
AmazonRedshift_node1750169156573 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
                        "useConnectionProperties": "true", 
                        "dbtable": "raw_data.bookings", 
                        "connectionName": "Redshift connection"}, 
    transformation_ctx="AmazonRedshift_node1750169156573")

# Script for node Change Schema
ChangeSchema_node1750167895434 = ApplyMapping.apply(
    frame=AmazonRedshift_node1750167535303, 
    mappings=[("id", "int", "id", "int"), 
              ("category", "string", "category", "string"), 
              ("body", "string", "body", "string"), 
              ("amenities", "string", "amenities", "string"), 
              ("bathrooms", "int", "bathrooms", "int"), 
              ("bedrooms", "int", "bedrooms", "int"), 
              ("fee", "decimal", "fee", "decimal"), 
              ("has_photo", "string", "has_photo", "string"), 
              ("pets_allowed", "string", "pets_allowed", "string"), 
              ("price_display", "string", "price_display", "string"), 
              ("price_type", "string", "price_type", "string"), 
              ("square_feet", "int", "square_feet", "int"), 
              ("address", "string", "address", "string"), 
              ("cityname", "string", "cityname", "string"), 
              ("state", "string", "state", "string"), 
              ("latitude", "decimal", "latitude", "decimal"), 
              ("longitude", "decimal", "longitude", "decimal")], 
    transformation_ctx="ChangeSchema_node1750167895434")

# Script for node Change Schema
ChangeSchema_node1750169177523 = ApplyMapping.apply(
    frame=AmazonRedshift_node1750169164676, 
    mappings=[("user_id", "int", "user_id", "int"), 
              ("apartment_id", "int", "apartment_id", "int"), 
              ("viewed_at", "string", "viewed_at", "varchar"), 
              ("is_wishlisted", "string", "is_wishlisted", "string"), 
              ("call_to_action", "string", "call_to_action", "string")], 
    transformation_ctx="ChangeSchema_node1750169177523")

# Script for node Change Schema
ChangeSchema_node1750169083768 = ApplyMapping.apply(
    frame=AmazonRedshift_node1750169072593, 
    mappings=[("id", "int", "id", "int"), 
              ("title", "string", "title", "string"), 
              ("source", "string", "source", "string"), 
              ("price", "decimal", "price", "decimal"), 
              ("currency", "string", "currency", "string"), 
              ("listing_created_on", "string", "listing_created_on", "varchar"), 
              ("is_active", "string", "is_active", "string"), 
              ("last_modified_timestamp", "string", "last_modified_timestamp", "varchar")], 
    transformation_ctx="ChangeSchema_node1750169083768")

# Script for node Change Schema
ChangeSchema_node1750169293027 = ApplyMapping.apply(
    frame=AmazonRedshift_node1750169156573, 
    mappings=[("booking_id", "int", "booking_id", "int"), 
              ("user_id", "int", "user_id", "int"), 
              ("apartment_id", "int", "apartment_id", "int"), 
              ("booking_date", "string", "booking_date", "varchar"), 
              ("checkin_date", "string", "checkin_date", "varchar"), 
              ("checkout_date", "string", "checkout_date", "varchar"), 
              ("total_price", "decimal", "total_price", "decimal"), 
              ("currency", "string", "currency", "string"), 
              ("booking_status", "string", "booking_status", "string")], 
    transformation_ctx="ChangeSchema_node1750169293027")

# Script for node Drop Duplicates
DropDuplicates_node1750167918104 =  DynamicFrame.fromDF(
    ChangeSchema_node1750167895434.toDF().dropDuplicates(), 
    glueContext, "DropDuplicates_node1750167918104")

# Script for node Drop Duplicates
DropDuplicates_node1750169188272 =  DynamicFrame.fromDF(
    ChangeSchema_node1750169177523.toDF().dropDuplicates(), 
    glueContext, "DropDuplicates_node1750169188272")

# Script for node Drop Duplicates
DropDuplicates_node1750169090359 =  DynamicFrame.fromDF(
    ChangeSchema_node1750169083768.toDF().dropDuplicates(), 
    glueContext, "DropDuplicates_node1750169090359")

# Script for node Drop Duplicates
DropDuplicates_node1750169298546 =  DynamicFrame.fromDF(
    ChangeSchema_node1750169293027.toDF().dropDuplicates(), 
    glueContext, "DropDuplicates_node1750169298546")

# Script for node Amazon Redshift
AmazonRedshift_node1750167986051 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1750167918104, 
    connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
                        "useConnectionProperties": "true", 
                        "dbtable": "curated.apartment_attributes", 
                        "connectionName": "Redshift connection", 
                        "preactions": "CREATE TABLE IF NOT EXISTS curated.apartment_attributes "
                        "(id INTEGER, "
                        "category VARCHAR, "
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
    transformation_ctx="AmazonRedshift_node1750167986051")

# Script for node Amazon Redshift
AmazonRedshift_node1750169198891 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1750169188272, 
    connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
                        "useConnectionProperties": "true", 
                        "dbtable": "curated.user_viewing", 
                        "connectionName": "Redshift connection", 
                        "preactions": "CREATE TABLE IF NOT EXISTS curated.user_viewing "
                        "(user_id INTEGER, "
                        "apartment_id INTEGER, "
                        "viewed_at VARCHAR, "
                        "is_wishlisted VARCHAR, "
                        "call_to_action VARCHAR);"}, 
    transformation_ctx="AmazonRedshift_node1750169198891")

# Script for node Amazon Redshift
AmazonRedshift_node1750169110317 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1750169090359, 
    connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
                        "useConnectionProperties": "true", 
                        "dbtable": "curated.apartments", 
                        "connectionName": "Redshift connection", 
                        "preactions": "CREATE TABLE IF NOT EXISTS curated.apartments "
                        "(id INTEGER, "
                        "title VARCHAR, "
                        "source VARCHAR, "
                        "price DECIMAL, "
                        "currency VARCHAR, "
                        "listing_created_on VARCHAR, "
                        "is_active VARCHAR, "
                        "last_modified_timestamp VARCHAR);"}, 
    transformation_ctx="AmazonRedshift_node1750169110317")

# Script for node Amazon Redshift
AmazonRedshift_node1750169355082 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1750169298546, 
    connection_type="redshift", 
    connection_options={"redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
                        "useConnectionProperties": "true", 
                        "dbtable": "curated.bookings", 
                        "connectionName": "Redshift connection", 
                        "preactions": "CREATE TABLE IF NOT EXISTS curated.bookings "
                        "(booking_id INTEGER, "
                        "user_id INTEGER, "
                        "apartment_id INTEGER, "
                        "booking_date VARCHAR, "
                        "checkin_date VARCHAR, "
                        "checkout_date VARCHAR, "
                        "total_price DECIMAL, "
                        "currency VARCHAR, "
                        "booking_status VARCHAR);"}, 
    transformation_ctx="AmazonRedshift_node1750169355082")

job.commit()
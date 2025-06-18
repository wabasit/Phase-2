import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script for node Relational DB
RelationalDB_node1750160099222 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartment_attributes",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750160099222"
)

# Script for node Relational DB
RelationalDB_node1750159568714 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "bookings",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750159568714"
)

# Script for node Relational DB
RelationalDB_node1750160085629 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "user_viewing",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750160085629"
)

# Script for node Relational DB
RelationalDB_node1750157458667 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "apartments",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "RelationalDB_node1750157458667"
)

# Script for node Amazon S3
EvaluateDataQuality().process_rows(frame=RelationalDB_node1750160099222, 
        ruleset=DEFAULT_DATA_QUALITY_RULESET, 
        publishing_options={
            "dataQualityEvaluationContext": "EvaluateDataQuality_node1750158942478", 
            "enableDataQualityResultsPublishing": True
        }, 
        additional_options={
            "dataQualityResultsPublishing.strategy": "BEST_EFFORT", 
            "observations.scope": "ALL"
        }
    )
AmazonS3_node1750160103884 = glueContext.write_dynamic_frame.from_options(
    frame=RelationalDB_node1750160099222, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={"path": "s3://project2dt/data/apartment_attributes/", 
                        "partitionKeys": []}, 
    format_options={"compression": "snappy"}, 
    transformation_ctx="AmazonS3_node1750160103884"
)

# Script for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=RelationalDB_node1750159568714, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1750158942478", 
        "enableDataQualityResultsPublishing": True
    }, additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT", 
        "observations.scope": "ALL"
    }
)
AmazonS3_node1750160001855 = glueContext.write_dynamic_frame.from_options(
    frame=RelationalDB_node1750159568714, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={
        "path": "s3://project2dt/data/bookings/", "partitionKeys": []
    }, format_options={
        "compression": "snappy"
    }, transformation_ctx="AmazonS3_node1750160001855")

# Script for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=RelationalDB_node1750160085629, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1750158942478", 
        "enableDataQualityResultsPublishing": True
    }, additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT", 
        "observations.scope": "ALL"
    })
AmazonS3_node1750160091195 = glueContext.write_dynamic_frame.from_options(
    frame=RelationalDB_node1750160085629, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={
        "path": "s3://project2dt/data/user_view/", 
        "partitionKeys": []
    }, format_options={
        "compression": "snappy"
    }, transformation_ctx="AmazonS3_node1750160091195")

# Script for node Amazon S3
EvaluateDataQuality().process_rows(
    frame=RelationalDB_node1750157458667, 
    ruleset=DEFAULT_DATA_QUALITY_RULESET, 
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1750156671565", 
        "enableDataQualityResultsPublishing": True
    }, additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT", 
        "observations.scope": "ALL"
    })
AmazonS3_node1750157473916 = glueContext.write_dynamic_frame.from_options(
    frame=RelationalDB_node1750157458667, 
    connection_type="s3", 
    format="glueparquet", 
    connection_options={
        "path": "s3://project2dt/data/apartments/", 
        "partitionKeys": []
    }, format_options={
        "compression": "snappy"
    }, transformation_ctx="AmazonS3_node1750157473916")

job.commit()
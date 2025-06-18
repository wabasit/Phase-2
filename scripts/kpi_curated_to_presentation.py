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

"""
This script is designed to extract data from various sources, 
transform it, and load it into Amazon Redshift for further analysis.
It includes data from apartment attributes, bookings, user viewing, and 
apartments, and performs various aggregations and calculations to generate 
key performance indicators (KPIs)."""
# Script for node Amazon Redshift
AmazonRedshift_node1750178830416 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={
        "sampleQuery": "WITH confirmed_bookings AS "
        "(SELECT apartment_id, TO_DATE(checkin_date, 'YYYY-MM-DD') AS checkin,"
        "TO_DATE(checkout_date, 'YYYY-MM-DD') AS checkout FROM curated.bookings"
        " WHERE LOWER(booking_status) LIKE '%confirmed%' ), "
        "booked_nights AS (SELECT DATE_TRUNC('month', checkin) AS month_start,         "
        "SUM(DATEDIFF(day, checkin, checkout)) AS total_booked_nights "
        "FROM confirmed_bookings GROUP BY 1 ), active_apartments AS "
        "(SELECT DATE_TRUNC('month', TO_DATE(listing_created_on, 'YYYY-MM-DD')) AS month_start,"
        "COUNT(*) AS active_apartments"
        "FROM curated.apartments"
        "WHERE LOWER(is_active) = 'true'"
        "GROUP BY 1 ), "
        "monthly_occupancy AS (SELECT TO_CHAR(bn.month_start, 'YYYYMM')::INT AS month,"
        "ROUND((bn.total_booked_nights::DECIMAL / NULLIF(aa.active_apartments * 30, 0)) * 100, 2) AS occupancy_rate "
        "FROM booked_nights bn "
        "JOIN active_apartments aa ON bn.month_start = aa.month_start ) "
        "SELECT * "
        "FROM monthly_occupancy "
        "ORDER BY month", 
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "connectionName": "Redshift connection"}, 
    transformation_ctx="AmazonRedshift_node1750178830416")

# Script for node Amazon Redshift
AmazonRedshift_node1750177712597 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={
        "sampleQuery": "SELECT TO_CHAR(DATE_TRUNC('week', TO_DATE(checkin_date, 'YYYY-MM-DD')),"
        "'YYYY-MM-DD') AS booking_date,     "
        "ROUND(AVG(DATEDIFF(day, TO_DATE(checkin_date, 'YYYY-MM-DD'), "
        "TO_DATE(checkout_date, 'YYYY-MM-DD'))), 2) AS avg_booking_duration "
        "FROM curated.bookings "
        "WHERE LOWER(booking_status) LIKE '%confirmed%' "
        "GROUP BY 1 "
        "ORDER BY 1", 
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "connectionName": "Redshift connection"}, 
    transformation_ctx="AmazonRedshift_node1750177712597")

# Script for node Amazon Redshift
AmazonRedshift_node1750179005739 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={
        "sampleQuery": "SELECT TO_CHAR(DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')), 'IYYYIW')::INT AS week_num,"
        "apartment_id, ROUND(SUM(total_price), 2) AS weekly_revenue "
        "FROM curated.bookings WHERE LOWER(booking_status) LIKE '%confirmed%' "
        "GROUP BY week_num, apartment_id "
        "ORDER BY week_num, apartment_id", 
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "connectionName": "Redshift connection"
    }, 
    transformation_ctx="AmazonRedshift_node1750179005739")

# Script for node Amazon Redshift
AmazonRedshift_node1750177246572 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={
        "sampleQuery": "SELECT EXTRACT(week FROM TO_DATE(booking_date, 'YYYY-MM-DD')) AS week_num,"
        "user_id, COUNT(*) AS total_bookings "
        "FROM curated.bookings "
        "WHERE LOWER(booking_status) LIKE '%confirmed%' "
        "GROUP BY 1, user_id "
        "ORDER BY 1, user_id", 
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", "connectionName": "Redshift connection"}, 
    transformation_ctx="AmazonRedshift_node1750177246572")

# Script for node Amazon Redshift
AmazonRedshift_node1750179026406 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={
        "sampleQuery": "SELECT TO_CHAR(DATE_TRUNC('week', TO_DATE(b.booking_date, 'YYYY-MM-DD')), 'IYYYIW')::INT AS week_num, "
        "aa.cityname, COUNT(*) AS total_bookings "
        "FROM curated.bookings b "
        "JOIN curated.apartment_attributes aa ON b.apartment_id = aa.id "
        "WHERE LOWER(b.booking_status) LIKE '%confirmed%' "
        "GROUP BY week_num, aa.cityname "
        "ORDER BY week_num, total_bookings DESC", 
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "connectionName": "Redshift connection"
    }, 
    transformation_ctx="AmazonRedshift_node1750179026406")

# Script for node Amazon Redshift
AmazonRedshift_node1750173372376 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={
        "sampleQuery": "SELECT EXTRACT(week FROM TO_DATE(listing_created_on, 'YYYY-MM-DD')) AS week_num, "
        "ROUND(AVG(price), 2) AS avg_listing_price "
        "FROM curated.apartments "
        "WHERE is_active = 'TRUE' "
        "GROUP BY 1 "
        "ORDER BY 1", 
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "connectionName": "Redshift connection"
    }, 
    transformation_ctx="AmazonRedshift_node1750173372376")

# Script for node Amazon Redshift
AmazonRedshift_node1750178285941 = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift", 
    connection_options={
        "sampleQuery": "WITH user_bookings AS ("
        "SELECT user_id, TO_DATE(booking_date, 'YYYY-MM-DD') AS booking_date, "
        "LEAD(TO_DATE(booking_date, 'YYYY-MM-DD')) "
        "OVER (PARTITION BY user_id ORDER BY TO_DATE(booking_date, 'YYYY-MM-DD')) AS next_booking_date "
        "FROM curated.bookings "
        "WHERE LOWER(booking_status) LIKE '%confirmed%' ), "
        "repeat_customers AS (     "
        "SELECT DISTINCT user_id "
        "FROM user_bookings "
        "WHERE next_booking_date IS NOT NULL "
        "AND DATEDIFF(day, booking_date, next_booking_date) <= 30 ), monthly_totals AS ( "
        "SELECT "
        "TO_CHAR(DATE_TRUNC('month', TO_DATE(booking_date, 'YYYY-MM-DD')), 'YYYY-MM') AS month, "
        "COUNT(DISTINCT user_id) AS total_customers "
        "FROM curated.bookings "
        "WHERE LOWER(booking_status) LIKE '%confirmed%' "
        "GROUP BY 1 ), monthly_repeats AS ( "
        "SELECT "
        "TO_CHAR(DATE_TRUNC('month', TO_DATE(b.booking_date, 'YYYY-MM-DD')), 'YYYY-MM') AS month, "
        "COUNT(DISTINCT b.user_id) AS repeat_customers "
        "FROM curated.bookings b "
        "JOIN repeat_customers rc ON b.user_id = rc.user_id "
        "WHERE LOWER(b.booking_status) LIKE '%confirmed%' "
        "GROUP BY 1 ) "
        "SELECT mt.month, "
        "ROUND((COALESCE(mr.repeat_customers, 0)::DECIMAL / NULLIF(mt.total_customers, 0)) * 100, 2) AS repeat_customer_rate "
        "FROM monthly_totals mt "
        "LEFT JOIN monthly_repeats mr ON mt.month = mr.month "
        "ORDER BY 1", 
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "connectionName": "Redshift connection"
    }, 
    transformation_ctx="AmazonRedshift_node1750178285941")

# Script for node Amazon Redshift
AmazonRedshift_node1750178834838 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonRedshift_node1750178830416, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "presentation.monthly_occupancy_rate", 
        "connectionName": "Redshift connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS presentation.monthly_occupancy_rate ("
        "month INTEGER, occupancy_rate DECIMAL);"
    }, 
    transformation_ctx="AmazonRedshift_node1750178834838"
)

# Script for node Amazon Redshift
AmazonRedshift_node1750177720033 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonRedshift_node1750177712597, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "presentation.avg_booking_duration", 
        "connectionName": "Redshift connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS presentation.avg_booking_duration ("
        "booking_date VARCHAR, avg_booking_duration DECIMAL);"
    }, 
    transformation_ctx="AmazonRedshift_node1750177720033"
)

# Script for node Amazon Redshift
AmazonRedshift_node1750179011997 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonRedshift_node1750179005739, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "presentation.top_performing_listings", 
        "connectionName": "Redshift connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS presentation.top_performing_listings ("
        "week_num INTEGER, apartment_id INTEGER, weekly_revenue DECIMAL);"
    }, 
    transformation_ctx="AmazonRedshift_node1750179011997"
)

# Script for node Amazon Redshift
AmazonRedshift_node1750177262771 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonRedshift_node1750177246572, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "presentation.total_bookings_per_user", 
        "connectionName": "Redshift connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS presentation.total_bookings_per_user ("
        "week_num INTEGER, user_id INTEGER, total_bookings BIGINT);"
    }, 
    transformation_ctx="AmazonRedshift_node1750177262771"
)

# Script for node Amazon Redshift
AmazonRedshift_node1750179036786 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonRedshift_node1750179026406, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "presentation.popular_locations", 
        "connectionName": "Redshift connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS presentation.popular_locations ("
        "week_num INTEGER, cityname VARCHAR, total_bookings BIGINT);"
    }, 
    transformation_ctx="AmazonRedshift_node1750179036786"
)

# Script for node Amazon Redshift
AmazonRedshift_node1750173432861 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonRedshift_node1750173372376, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "presentation.avg_listing_price", 
        "connectionName": "Redshift connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS presentation.avg_listing_price ("
        "week_num INTEGER, avg_listing_price DECIMAL);"
    }, 
    transformation_ctx="AmazonRedshift_node1750173432861"
)

# Script for node Amazon Redshift
AmazonRedshift_node1750178291373 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonRedshift_node1750178285941, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-025523568662-eu-north-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "presentation.repeat_customer_rate", 
        "connectionName": "Redshift connection", 
        "preactions": "CREATE TABLE IF NOT EXISTS presentation.repeat_customer_rate ("
        "month VARCHAR, repeat_customer_rate DECIMAL);"
    }, 
    transformation_ctx="AmazonRedshift_node1750178291373"
)

job.commit()
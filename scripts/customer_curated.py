import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Customer Trusted
CustomerTrusted_node1739966277126 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1739966277126")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1739966310043 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1739966310043")

# Script generated for node SQL Query
SqlQuery5294 = '''
SELECT DISTINCT c.* 
FROM customer_trusted c
INNER JOIN accelerometer_trusted a ON c.email = a.user

'''
SQLQuery_node1739966356396 = sparkSqlQuery(glueContext, query = SqlQuery5294, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1739966310043, "customer_trusted":CustomerTrusted_node1739966277126}, transformation_ctx = "SQLQuery_node1739966356396")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1739966356396, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739966066353", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739966397540 = glueContext.getSink(path="s3://stedi-project-shubham/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739966397540")
AmazonS3_node1739966397540.setCatalogInfo(catalogDatabase="stedi_landing",catalogTableName="customer_curated")
AmazonS3_node1739966397540.setFormat("json")
AmazonS3_node1739966397540.writeFrame(SQLQuery_node1739966356396)
job.commit()
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
CustomerTrusted_node1739964320426 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1739964320426")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1739963980690 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1739963980690")

# Script generated for node trusted_acc_query
SqlQuery4718 = '''
SELECT a.* 
FROM accelerometer_landing a
INNER JOIN customer_trusted c ON a.user = c.email
'''
trusted_acc_query_node1739964194922 = sparkSqlQuery(glueContext, query = SqlQuery4718, mapping = {"accelerometer_landing":AccelerometerLanding_node1739963980690, "customer_trusted":CustomerTrusted_node1739964320426}, transformation_ctx = "trusted_acc_query_node1739964194922")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=trusted_acc_query_node1739964194922, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739964078660", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739964388846 = glueContext.getSink(path="s3://stedi-project-shubham/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739964388846")
AmazonS3_node1739964388846.setCatalogInfo(catalogDatabase="stedi_landing",catalogTableName="accelerometer_trusted")
AmazonS3_node1739964388846.setFormat("json")
AmazonS3_node1739964388846.writeFrame(trusted_acc_query_node1739964194922)
job.commit()
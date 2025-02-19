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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1739964487635 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1739964487635")

# Script generated for node Customer Trusted
CustomerTrusted_node1739964528109 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1739964528109")

# Script generated for node SQL Query
SqlQuery5166 = '''
SELECT s.*
FROM step_trainer_landing s
INNER JOIN customer_trusted c ON s.serialNumber = c.serialnumber
'''
SQLQuery_node1739964687744 = sparkSqlQuery(glueContext, query = SqlQuery5166, mapping = {"step_trainer_landing":StepTrainerLanding_node1739964487635, "customer_trusted":CustomerTrusted_node1739964528109}, transformation_ctx = "SQLQuery_node1739964687744")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1739964687744, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739964078660", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739964747771 = glueContext.getSink(path="s3://stedi-project-shubham/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739964747771")
AmazonS3_node1739964747771.setCatalogInfo(catalogDatabase="stedi_landing",catalogTableName="step_trainer_trusted")
AmazonS3_node1739964747771.setFormat("json")
AmazonS3_node1739964747771.writeFrame(SQLQuery_node1739964687744)
job.commit()
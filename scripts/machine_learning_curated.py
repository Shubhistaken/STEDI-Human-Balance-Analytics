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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1739966546253 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1739966546253")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1739966579775 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1739966579775")

# Script generated for node SQL Query
SqlQuery5072 = '''
SELECT s.*, a.*
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a ON s.sensorReadingTime = a.timestamp

'''
SQLQuery_node1739966603820 = sparkSqlQuery(glueContext, query = SqlQuery5072, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1739966579775, "step_trainer_trusted":StepTrainerTrusted_node1739966546253}, transformation_ctx = "SQLQuery_node1739966603820")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1739966603820, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739967071546", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739967250822 = glueContext.getSink(path="s3://stedi-project-shubham/ml_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739967250822")
AmazonS3_node1739967250822.setCatalogInfo(catalogDatabase="stedi_landing",catalogTableName="machine_learning_curated")
AmazonS3_node1739967250822.setFormat("json")
AmazonS3_node1739967250822.writeFrame(SQLQuery_node1739966603820)
job.commit()
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

# Script generated for node Customer Landing
CustomerLanding_node1739961257371 = glueContext.create_dynamic_frame.from_catalog(database="stedi_landing", table_name="customer_landing", transformation_ctx="CustomerLanding_node1739961257371")

# Script generated for node share_with_research_query
SqlQuery5425 = '''
SELECT * FROM customer_landing WHERE shareWithResearchAsOfDate IS NOT NULL

'''
share_with_research_query_node1739961378796 = sparkSqlQuery(glueContext, query = SqlQuery5425, mapping = {"customer_landing":CustomerLanding_node1739961257371}, transformation_ctx = "share_with_research_query_node1739961378796")

# Script generated for node save_to_s3
EvaluateDataQuality().process_rows(frame=share_with_research_query_node1739961378796, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739961213409", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
save_to_s3_node1739961539293 = glueContext.getSink(path="s3://stedi-project-shubham/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="save_to_s3_node1739961539293")
save_to_s3_node1739961539293.setCatalogInfo(catalogDatabase="stedi_landing",catalogTableName="customer_trusted")
save_to_s3_node1739961539293.setFormat("json")
save_to_s3_node1739961539293.writeFrame(share_with_research_query_node1739961378796)
job.commit()
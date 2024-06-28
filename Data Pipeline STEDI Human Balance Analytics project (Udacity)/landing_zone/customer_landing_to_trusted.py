import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node customer landing
customerlanding_node1713449378964 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project-bucket-282192/customer/landing/"], "recurse": True}, transformation_ctx="customerlanding_node1713449378964")

# Script generated for node Share with Research
SqlQuery0 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null;
'''
SharewithResearch_node1713449746210 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customerlanding_node1713449378964}, transformation_ctx = "SharewithResearch_node1713449746210")

# Script generated for node customer trusted
customertrusted_node1713449911936 = glueContext.getSink(path="s3://stedi-project-bucket-282192/customer/trusted_parquet/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customertrusted_node1713449911936")
customertrusted_node1713449911936.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted_parquet")
customertrusted_node1713449911936.setFormat("glueparquet", compression="snappy")
customertrusted_node1713449911936.writeFrame(SharewithResearch_node1713449746210)
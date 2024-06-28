import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1713457752834 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-project-bucket-282192/accelerometer/trusted_parquet/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1713457752834")

# Script generated for node customer_trusted
customer_trusted_node1713455977483 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-project-bucket-282192/customer/trusted_parquet/"], "recurse": True}, transformation_ctx="customer_trusted_node1713455977483")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT c.*
FROM customer_trusted c 
JOIN accelerometer_trusted a  ON c.email = a.user;
'''
SQLQuery_node1713455790980 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":customer_trusted_node1713455977483, "accelerometer_trusted":accelerometer_trusted_node1713457752834}, transformation_ctx = "SQLQuery_node1713455790980")

# Script generated for node Drop Duplicates
DropDuplicates_node1713458145999 =  DynamicFrame.fromDF(SQLQuery_node1713455790980.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1713458145999")

# Script generated for node customer_curated
customer_curated_node1713455185800 = glueContext.getSink(path="s3://stedi-project-bucket-282192/customer/curated_parquet/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1713455185800")
customer_curated_node1713455185800.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated_parquet")
customer_curated_node1713455185800.setFormat("glueparquet", compression="snappy")
customer_curated_node1713455185800.writeFrame(DropDuplicates_node1713458145999)
job.commit()
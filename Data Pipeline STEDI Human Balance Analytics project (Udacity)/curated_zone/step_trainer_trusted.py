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

# Script generated for node step trainer landing
steptrainerlanding_node1713454064577 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project-bucket-282192/step_trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1713454064577")

# Script generated for node customer_curated
customer_curated_node1713454113747 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-project-bucket-282192/customer/curated_parquet/"]}, transformation_ctx="customer_curated_node1713454113747")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT s.*
FROM step_trainer_landing s
JOIN customer_curated c ON c.serialnumber = s.serialnumber;
'''
SQLQuery_node1713454183322 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":steptrainerlanding_node1713454064577, "customer_curated":customer_curated_node1713454113747}, transformation_ctx = "SQLQuery_node1713454183322")

# Script generated for node Drop Duplicates
DropDuplicates_node1713454583330 =  DynamicFrame.fromDF(SQLQuery_node1713454183322.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1713454583330")

# Script generated for node step trainer trusted
steptrainertrusted_node1713454274607 = glueContext.getSink(path="s3://stedi-project-bucket-282192/step_trainer/trusted_parquet/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1713454274607")
steptrainertrusted_node1713454274607.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted_parquet")
steptrainertrusted_node1713454274607.setFormat("glueparquet", compression="snappy")
steptrainertrusted_node1713454274607.writeFrame(DropDuplicates_node1713454583330)
job.commit()
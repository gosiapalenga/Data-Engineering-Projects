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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1713458836960 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-project-bucket-282192/step_trainer/trusted_parquet/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1713458836960")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1713458767401 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedi-project-bucket-282192/accelerometer/trusted_parquet/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1713458767401")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * 
FROM step_trainer_trusted s
JOIN accelerometer_trusted a on s.sensorreadingtime = a.timestamp;

'''
SQLQuery_node1713458876509 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":step_trainer_trusted_node1713458836960, "accelerometer_trusted":accelerometer_trusted_node1713458767401}, transformation_ctx = "SQLQuery_node1713458876509")

# Script generated for node machine_learning_curated
machine_learning_curated_node1713459024546 = glueContext.getSink(path="s3://stedi-project-bucket-282192/machine_learning_parquet/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1713459024546")
machine_learning_curated_node1713459024546.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_parquet")
machine_learning_curated_node1713459024546.setFormat("glueparquet", compression="snappy")
machine_learning_curated_node1713459024546.writeFrame(SQLQuery_node1713458876509)
job.commit()
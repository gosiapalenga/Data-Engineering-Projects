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

# Script generated for node accelerometer landing
accelerometerlanding_node1713453514250 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project-bucket-282192/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1713453514250")

# Script generated for node customer landing
customerlanding_node1713453476529 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project-bucket-282192/customer/landing/"], "recurse": True}, transformation_ctx="customerlanding_node1713453476529")

# Script generated for node SQL Query
SqlQuery0 = '''
select a.* from accelerometer_landing a
join customer_landing c  
on a.user = c.email
where c.sharewithresearchasofdate is not null;
'''
SQLQuery_node1713453546208 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":accelerometerlanding_node1713453514250, "customer_landing":customerlanding_node1713453476529}, transformation_ctx = "SQLQuery_node1713453546208")

# Script generated for node accelerometer trusted
accelerometertrusted_node1713453611403 = glueContext.getSink(path="s3://stedi-project-bucket-282192/accelerometer/trusted_parquet/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1713453611403")
accelerometertrusted_node1713453611403.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted_parquet")
accelerometertrusted_node1713453611403.setFormat("glueparquet", compression="snappy")
accelerometertrusted_node1713453611403.writeFrame(SQLQuery_node1713453546208)
job.commit()
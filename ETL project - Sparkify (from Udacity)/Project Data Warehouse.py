#!/usr/bin/env python
# coding: utf-8

# In[40]:


import boto3
import json
import psycopg2
import pandas as pd
import configparser

from botocore.exceptions import ClientError
get_ipython().run_line_magic('load_ext', 'sql')


# ### 1. Read AWS parameters

# In[23]:


config= configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

ACCESS_KEY  = config.get('default','aws_access_key_id')
SECRET_KEY  = config.get('default','aws_secret_access_key')


BUCKET_NAME  = config.get('S3','BUCKET_NAME')
LOG_DATA     = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA    = config.get('S3','SONG_DATA')

IAM_ROLE_NAME  = config.get('IAM','IAM_ROLE_NAME')
S3_ACCESS_ROLE_ARN = config.get('IAM','S3_ACCESS_ROLE_ARN') 


HOST        = config.get('CLUSTER','HOST')
DB_NAME     = config.get('CLUSTER','DB_NAME')
DB_USER     = config.get('CLUSTER','DB_USER')
DB_PASSWORD = config.get('CLUSTER','DB_PASSWORD')
DB_PORT     = config.get('CLUSTER','DB_PORT')


# ### 2. Create clients

# In[24]:


ec2_client = boto3.client('ec2',
                       region_name="us-east-1"
                    )

s3_client = boto3.client('s3',
                       region_name="us-east-1"
                   )

# Create S3 resource
s3_resource = boto3.resource('s3')


iam_client = boto3.client('iam',
                       region_name='us-east-1'
                     
                  )

redshift_client = boto3.client('redshift',
                       region_name="us-east-1"
                       )


# ### 3. Create a Role which will be used by Redshift to access S3

# In[25]:


assume_policy = json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})


try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam_client.create_role(
        Path='/',
        RoleName= 'RedshiftRole',
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=assume_policy
    )    
except Exception as e:
    print("Error creating role", e)
    

    
try:
    print("1.2 Attaching Policy")
    iam_client.attach_role_policy(RoleName='RedshiftRole',
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
except Exception as e:
    print("Error attaching policy")

    
print("1.3 Get the IAM role ARN")
IAM_ROLE_ARN = iam_client.get_role(RoleName= 'RedshiftRole')['Role']['Arn']

print(IAM_ROLE_ARN)


# ### 4. Create security group

# Provide your default VPC_ID

# In[64]:


# Create security group by passing vpc_id nd group name. The security group will allow Redshift port 5439

SG_GROUP_NAME = 'redshift-security-group'
SG_GROUP_DESCRIPTION = 'Security group for Redshift cluster access'
VPC_ID = "vpc-111122222333344"

try:
    response = ec2_client.create_security_group(
         GroupName = SG_GROUP_NAME,
         Description = SG_GROUP_DESCRIPTION,
         VpcId = VPC_ID
    )
        
    security_group_id = response['GroupId']
    print('Created security group with ID:', security_group_id)
except ClientError as e:
    if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
        # The security group already exists
        response = ec2_client.describe_security_groups(
                    Filters=[
                        {'Name': 'group-name', 'Values': [SG_GROUP_NAME]},
                        {'Name': 'vpc-id', 'Values': [VPC_ID]}
                    ])
        security_group_id = response['SecurityGroups'][0]['GroupId']
        print("Security group already exists. Using existing security group with ID:", security_group_id)
    else:
        # Handle other exceptions
        print("Error creating security group:", e)
        


# ### 5. Create inbound rule for security group

# In[65]:


print(security_group_id)

PORT = 5439
IP_RANGE = '0.0.0.0/0'

try:
    # Add the inbund rule to the security group
    response = ec2_client.authorize_security_group_ingress(
        GroupId = security_group_id,
        IpPermissions = [{
            'IpProtocol': 'tcp',
            'FromPort': PORT,
            'ToPort': PORT,
            'IpRanges': [{'CidrIp': IP_RANGE}]
        }])
    print('Inbound rule added to the security group.')
except ClientError as e:
    if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
        print('Inbound rule already exists for the specified port and IP range.')
    else:
        print("Error adding inbound rule:", e)


# ### 6. Check VPC and subnets

# In[66]:


response = ec2_client.describe_subnets()
response


# In[67]:


for subnet in response['Subnets']:
    print('subnet_id:', subnet['SubnetId'])
    print('VPC_ID:', subnet['VpcId'])
    print('CIDR Block:', subnet['CidrBlock'])
    print('Availability Zone:', subnet['AvailabilityZone'])
    print("###########")


# ### 7. Create a subnet group

# In[ ]:


# The subnet group represents a group of subnets. 

# When creating a Redshift cluster, we are required to associate the cluster with a subnet group.
# Deploying a cluster in multiple subnets allows to distribute the cluster accross different AZs.

# If we have data sources or clients in different regions or AZs, placing cluster in multiple subnets closer to
# those data sources or clients helps reducing daat transfer cost.


# In[68]:


SUBNET_GROUP_NAME = 'redshift-subnet-group'

SUBNET_IDs = ['subnet-0f8918c734a8baa2e',
              'subnet-097b1642c7a093221',
              'subnet-0345435ad6dd4d7b8',
              'subnet-011b668868cdd0f37',
              'subnet-09c39f23e9f72c359',
              'subnet-03d875a6253a8dff2'
             ]

try:
    response = redshift_client.create_cluster_subnet_group(
        ClusterSubnetGroupName = SUBNET_GROUP_NAME,
        Description = 'Subnet group for Redshift cluster',
        SubnetIds = SUBNET_IDs   
    )
    print(SUBNET_GROUP_NAME)
    print('Subnet group created successfully.')
except ClientError as e:
    if e.response['Error']['Code'] == 'ClusterSubnetGroupAlreadyExists':
        print('Subnet group already exists. Skipping creation.')
    else:
        print('Error creating subnet group:', e)


# ### 8. Define Redshift cluster parameters

# Provide MasterUsername, MasterUserPassword

# In[31]:


cluster_parameters = {
    'ClusterIdentifier': 'project-redshift-cluster',
    'NodeType': 'dc2.large',
    'MasterUsername': '',
    'MasterUserPassword': '',
    'DBName': 'sparkify_database',
    'ClusterType': 'single-node',
    'NumberOfNodes': 1,
    'PubliclyAccessible': True,                 
    'VpcSecurityGroupIds': [security_group_id],
    'AvailabilityZone': 'us-east-1a',          
    'Port': 5439,
    'ClusterSubnetGroupName': SUBNET_GROUP_NAME
}


# ### 9. Create Redshift cluster

# In[69]:


try:
    response = redshift_client.create_cluster(**cluster_parameters)
    print('Redshift cluster creation initiated.')
except redshift_client.exceptions.ClusterAlreadyExistsFault:
    print('Cluster already exists. Skipping cluster creation.')
    

# ClusterIdentifier parameter specifies the unique id for Redshift cluster
# The redshift_client.get_waiter('cluster_available').wait() statement waits until the Redshift cluster becomes available.
# By default, it'll continuously check the cluster status until it becomes available.


redshift_client.get_waiter('cluster_available').wait(
    ClusterIdentifier = cluster_parameters['ClusterIdentifier']
)

print('Redshift cluster is now available')


# ### 10. Describe Redshift cluster

# In[70]:


cluster_info = redshift_client.describe_clusters(ClusterIdentifier=cluster_parameters['ClusterIdentifier'])['Clusters'][0]
cluster_info


# ### 11. Associate Role with cluster

# In[71]:


# DWH_CLUSTER_NAME = 'project-redshift-cluster'

redshift_client.modify_cluster_iam_roles(
    ClusterIdentifier = cluster_parameters['ClusterIdentifier'],
    AddIamRoles = [IAM_ROLE_ARN]
)

print("This role will be granted permissions to access S3 within the cluster")


# ### 12. Connect to Redshift cluster

# Provide your username, password and db name

# In[72]:


redshift_endpoint = cluster_info['Endpoint']['Address']
redshift_port = 5439
redshift_user = ''
redshift_password = ''
redshift_database = 'sparkify_database'
redshift_host=cluster_info['Endpoint']['Address']

try:
    conn = psycopg2.connect(host=redshift_host,
                            port=redshift_port,
                            database=redshift_database,
                            user=redshift_user,
                            password=redshift_password                           
                           )
    print("Successfully connected to Redshift cluster")
except psycopg2.Error as e:
    print("Error connecting to Redshift cluster,", e)


# In[36]:


cursor = conn.cursor()
conn.set_session(autocommit = True)


# ### 13. Create Bucket for the project

# In[ ]:


project_bucket = s3_client.create_bucket(Bucket = 'dwhproject282192')


# #### Extract data from udacity-dend Bucket

# #### Get log metadata

# In[ ]:


# I used aws cli to copy the files between the buckets
# aws s3 cp s3://udacity-dend/log_json_path.json s3://dwhproject282192/


# #### Get Log data

# In[ ]:


# I used aws cli to copy the files between the buckets
# aws s3 sync s3://udacity-dend/log_data s3://dwhproject282192/log_data/


# #### Get song data

# In[ ]:


# I used aws cli to copy the files between the buckets
# aws s3 sync s3://udacity-dend/song_data s3://dwhproject282192/song_data/


# ### 14. Run python scripts

# In[73]:


get_ipython().run_line_magic('run', 'create_tables.py')


# In[74]:


get_ipython().run_line_magic('run', 'etl.py')


# ### 15. Test tables

# In[75]:


# connect to database

conn_string=f"postgresql://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_database}"
get_ipython().run_line_magic('sql', '$conn_string')


# Check staging tables

# In[76]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT COUNT(*) as total_rows\nFROM public.staging_log_data;\n')


# In[77]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT COUNT(*) as total_rows\nFROM public.staging_songs_data;\n')


# Check Analysis tables

# In[78]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT COUNT(*) as total\nFROM users;\n')


# In[79]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT COUNT(*) as total\nFROM songs;\n')


# In[80]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT COUNT(*) as total\nFROM artists;\n')


# In[81]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT COUNT(*) as total\nFROM time;\n')


# In[ ]:


get_ipython().run_cell_magic('time', '', '%%sql\nSELECT COUNT(*)\nFROM songplays;\n')


# ### 16. Clean-up

# In[88]:


# Delete cluster
redshift_client.delete_cluster(
                                ClusterIdentifier=cluster_parameters['ClusterIdentifier'],
                                SkipFinalClusterSnapshot=True)
print('Redshift cluster deletion initiated.')

redshift_client.get_waiter('cluster_deleted').wait(ClusterIdentifier=cluster_parameters['ClusterIdentifier'])
print("Successfully deleted cluster")


# In[89]:


# Delete subnet group
redshift_client.delete_cluster_subnet_group(ClusterSubnetGroupName=SUBNET_GROUP_NAME)


# In[90]:


# Delete security group
ec2_client.delete_security_group(GroupId=security_group_id)


# In[91]:


# Empty the project bucket
bucket = s3_resource.Bucket('dwhproject282192')
bucket.objects.delete()


# In[92]:


# Delete the bucket
bucket.delete()


# In[93]:


# Detach and delete role
iam_client.detach_role_policy(RoleName='RedshiftRole', PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
iam_client.delete_role(RoleName='RedshiftRole')


# In[ ]:





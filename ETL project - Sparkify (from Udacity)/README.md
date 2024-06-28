# Data-Engineering-project---Sparkify from Udacity

The purpose of this project is to build an ETL process, which creates tables and database in AWS Redshift cluster.<br />
The data is fetched from public S3 buckets which contain files in json format. The files from the buckets are extracted and loaded to staging tables using COPY command.<br />
Afterwards the data is transformed and loaded to the fact and dimension tables.<br />

### To start:<br />
1. Set up config file for AWS parameters.<br />
2. Design star schema for the tables, which are:<br /> 
	- staging tables:<br /> 
		* staging_log_data: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId<br />
		* staging_songs_data: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year<br />
	- fact and dimension tables:<br /> 
		* songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent<br />
		* users: user_id, first_name, last_name, gender, level<br />
		* songs: song_id, title, artist_id, year, duration<br />
		* artists: artist_id, name, location, latitude, longitude<br />
		* time: start_time, hour, day, week, month, year, weekday<br />
3. Write a SQL CREATE statement for each of these tables in sql_queries.py files.<br />
4. Complete the create_tables.py file to connect to the database, drop all tables and create these tables.<br />
5. Complete the etl.py file to load data from S3 to staging tables on Redshift and to load data from staging tables to analytics tables on Redshift.<br />
6. Follow the below steps to set up AWS environment to create the tables.<br />


![Sparkify_ER_diagram](https://github.com/gosiapalenga/Data-Engineering-project---Sparkify-Udacity/assets/22677245/fd0589ab-1ddd-4a90-ab4a-91037dd69415)

 
### In Jupyter Notebook create a script to:<br />

1. Read AWS parameters saved in the config and credentials files.<br />
2. Create Boto3 clients for EC2, S3, IAM, REDSHIFT to interact with those AWS services.<br />
3. Create IAM role which will be acquired by Redshift to read S3 bucket data. <br />
   The ARN of the role is requred to assign the role to Redshift.<br />
   
5. Create security group for the Redshift cluster.<br />
6. Create an inboud rule for the security group to allow public access.<br />
7. Read VPC and subnets info required to create a subnet group for the cluster.<br />
8. Create a subnet group.<br />
9. Define parameetrs for the Redshift cluster<br />
10. Create Redshift Cluster<br />
   ClusterIdentifier parameter specifies the unique id for Redshift cluster.<br />
   
   I used redshift_client.get_waiter('cluster_available').wait() statement, which waits until the Redshift cluster becomes available.
   By default, it'll continuously check the cluster status until it becomes available.
   
11. Print cluster information
12. Associate the IAM role with tthe cluster, so that it can read data from S3 buckets.
13. Connect to the Redshift cluster.
14. Create a bucket, which will store the data for this project.<br />
    The data comes from two public buckets: <br />
    Song data: s3://udacity-dend/song_data<br />
    Log data : s3://udacity-dend/log_data<br />
    Log data column names: s3://udacity-dend/log_json_path.json<br />
   
    Those buckets are stored in us-west-2 region.
	Due to the large number of files, I used AWS CLI to copy the files to my bucket.
	
15. Once the data has been transfered, I run create_tables.py script, which drops and create tables in the Redshift cluster.
	Afterwards, I run etl.py script which loads data from the bucket to the staging tables and from staging tables to Analysis tables.
	
16. Test the tables by running SQL queries.
17. Clean up created AWS resources.

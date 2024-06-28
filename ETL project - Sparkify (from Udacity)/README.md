The purpose of this project is to build an ETL process, which creates tables and database in AWS Redshift cluster. 
The data is fetched from pubclic S3 buckets which contain files in json format. The files from the buckets are extracted and loaded to staging tables using COPY command.
Afterwards the data is transformed and loaded to the fact and dimension tables.

To start:
	a) set up credentials and config files for AWS parameters.
	b) design star schema for the tables, which are: 
	   - staging tables : 
			* staging_log_data: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
			* staging_songs_data: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year
	   - fact and dimension tables : 
			* songplays: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
			* users: user_id, first_name, last_name, gender, level
			* songs: song_id, title, artist_id, year, duration
			* artists: artist_id, name, location, latitude, longitude
			* time: start_time, hour, day, week, month, year, weekday
	c) Write a SQL CREATE statement for each of these tables in sql_queries.py files.
	d) Complete the create_tables.py file to connect to the database, drop all tables and create these tables.
	e) Complete the etl.py file to load data from S3 to staging tables on Redshift and to load data from staging tables to analytics tables on Redshift.
	f) Follow the below steps to set up AWS environment to create the tables.
	
In Jupyter Notebook create a script to:

1. Read AWS parameters saved in the config and credentials files.

2. Create Boto3 clients for EC2, S3, IAM, REDSHIFT to interact with those AWS services.

3. Create IAM role which will be acquired by Redshift to read S3 bucket data. 
   The ARN of the role is requred to assign the role to Redshift.

4. Create security group for the Redshift cluster.

5. Create an inboud rule for the security group to allow public access.

6. Read VPC and subnets info required to create a subnet group for the cluster.

7. Create a subnet group.

8. Define parameetrs for the Redshift cluster

9. Create Redshift Cluster
   ClusterIdentifier parameter specifies the unique id for Redshift cluster.
   
   I used redshift_client.get_waiter('cluster_available').wait() statement, which waits until the Redshift cluster becomes available.
   By default, it'll continuously check the cluster status until it becomes available.
   
10. Print cluster information

11. Associate the IAM role with tthe cluster, so that it can read data from S3 buckets.

12. Connect to the Redshift cluster.

13. Create a bucket, which will store the data for this project.
    The data comes from two public buckets: 
    Song data: s3://udacity-dend/song_data
    Log data : s3://udacity-dend/log_data
    Log data column names: s3://udacity-dend/log_json_path.json
   
    Those buckets are stored in us-west-2 region.
	Due to the large number of files, I used AWS CLI to copy the files to my bucket.
	
14. Once the data has been transfered, I run create_tables.py script, which drops and create tables in the Redshift cluster.
	Afterwards, I run etl.py script which loads data from the bucket to the staging tables and from staging tables to Analysis tables.
	
15. Test the tables by running SQL queries on them.

16. Clean up created AWS resources.

    
    

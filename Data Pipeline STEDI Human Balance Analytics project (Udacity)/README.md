The purpose of this project is to build a lakehouse solution by processing JSON data stored in S3 buckets, categorizing it into zones/layers and curating using AWS Glue and Athena.

1. Configure the S3 VPC Gateway Endpoint

2. Create the Glue Service Role.

3. Create a database for the project tables

4. Create a project bucket and load the landing data.

5. Create the landing tables in Athena from S3 buckets.

6. Create ETL jobs in AWS Glue Studio to create trusted and curated tables from data stored in S3 buckets.

7. Copy Scripts and check row count for each table.

8. Three data zones and the row count per table

Landing
	Customer: 956
	Accelerometer: 81273
	Step Trainer: 28680
Trusted
	Customer: 482
	Accelerometer: 40981
	Step Trainer: 14460
Curated
	Customer: 482
	Machine Learning: 43681

9. Three data sources in a JSON format
** Customer Records contains the following fields:

serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate
customername
email
lastupdatedate
phone
sharewithfriendsasofdate

** Step Trainer Records contains the following fields:

sensorReadingTime
serialNumber
distanceFromObject

** Accelerometer Records contains the following fields:

timeStamp
user
x
y
z


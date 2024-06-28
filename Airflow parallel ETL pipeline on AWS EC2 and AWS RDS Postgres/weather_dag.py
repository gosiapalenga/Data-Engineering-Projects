from airflow import DAG   # this is telling the program that this file is our DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator       # dummy task
from airflow.utils.task_group import TaskGroup                   # needed for splitting tasks --> parallel processing
from airflow.providers.postgres.operators.postgres import PostgresOperator      # run in Terminal pip3 install 'apache-airflow[postgres]'
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import pandas as pd


# pull data that the previous task pulled (extract_weather_data) from inside the task group
def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="group_a.extract_weather_data")   

    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp = data["main"]["temp"]/10
    feels_like= (data["main"]["feels_like"])/10
    min_temp = data["main"]["temp_min"]/10
    max_temp = data["main"]["temp_max"]/10
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'])


    # put the data in a dictionary
    transformed_data = {"city": city,
                            "description": weather_description,
                            "temperature (C)": temp,
                            "feels Like (C)": feels_like,
                            "minimun Temp (C)":min_temp,
                            "maximum Temp (C)": max_temp,
                            "pressure": pressure,
                            "humidty": humidity,
                            "wind Speed": wind_speed,
                            "time of Record": time_of_record,
                            "sunrise (Local Time)":sunrise_time,
                            "sunset (Local Time)": sunset_time                        
                            }
   
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = "current_weather_data_houston_" + dt_string
    #df_data.to_csv(f"s3://open-weather-bucket-282192/{dt_string}.csv", index=False, storage_options=aws_credentials)
    df_data.to_csv("current_weather_data.csv", index=False, header=False)


def load_weather():
    hook = PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql= "COPY weather_data FROM stdin WITH DELIMITER as ','",
        filename='current_weather_data.csv'
    )


def save_joined_data_to_S3(task_instance):
    data = task_instance.xcom_pull(task_ids="join_data")
    df = pd.DataFrame(data, columns = ['city', 'description', 'temperature', 'feels_like', 'minimun_temp', 'maximum_temp', 'pressure','humidity', 'wind_speed', 'time_of_record', 'sunrise_local_time', 'sunset_local_time', 'state', 'census_2020', 'land_area_sq_mile_2020'])
    df.to_csv("joined_weather_data.csv", index=False)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'joined_weather_data_' + dt_string
    df.to_csv(f"s3://open-weather-bucket-282192/{dt_string}.csv", index=False)
    #df.to_csv("joined_weather_data.csv", index=False, header=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,5,9),
    'email': ['g@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


# start of my DAG instance
with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        start_pipeline = DummyOperator(
        task_id = 'start_pipeline'
        )

        # join the data from below Task Group, outside this task group.
        join_data = PostgresOperator(
                task_id='join_data',
                postgres_conn_id = "postgres_conn",
                sql= '''SELECT 
                    w.city,                    
                    description,
                    temperature,
                    feels_like,
                    minimun_temp,
                    maximum_temp,
                    pressure,
                    humidity,
                    wind_speed,
                    time_of_record,
                    sunrise_local_time,
                    sunset_local_time,
                    state,
                    census_2020,
                    land_area_sq_mile_2020                    
                    FROM weather_data w
                    INNER JOIN city_lookup c
                        ON w.city = c.city                                      
                ;
                '''
        )


        load_joined_data = PythonOperator(
           task_id = "load_joined_data",
           python_callable = save_joined_data_to_S3
        )


        end_pipeline = DummyOperator(
                task_id = 'end_pipeline'
        )

        # create a task group 
        with TaskGroup(group_id = 'group_a', tooltip = 'Extract_from_S3_and_weatherapi') as group_a:
            # task 1
            create_table_1 = PostgresOperator(
                task_id = 'create_table_1',
                postgres_conn_id = 'postgres_conn',
                sql = '''
                    CREATE TABLE IF NOT EXISTS city_lookup (
                    city TEXT NOT NULL,
                    state TEXT NOT NULL,
                    census_2020 numeric NOT NULL,
                    land_area_sq_mile_2020 numeric NOT NULL
                    );
                '''
            )


            # task 2
            truncate_table = PostgresOperator(
                task_id = 'truncate_table',
                postgres_conn_id = 'postgres_conn',
                sql = ''' TRUNCATE TABLE city_lookup;'''
            )


            # task 3
            upload_S3_to_postgres = PostgresOperator(
                task_id = "upload_S3_to_postgres",
                postgres_conn_id = "postgres_conn",
                sql = "SELECT aws_s3.table_import_from_s3('city_lookup', '', '(format csv, DELIMITER '','', HEADER true)', 'open-weather-bucket-282192', 'us_city.csv', 'us-east-1');"
            )


            # task 4
            create_table_2 = PostgresOperator(
                task_id='create_table_2',
                postgres_conn_id = "postgres_conn",
                sql= ''' 
                    CREATE TABLE IF NOT EXISTS weather_data (
                    city TEXT,
                    description TEXT,
                    temperature NUMERIC,
                    feels_like NUMERIC,
                    minimun_temp NUMERIC,
                    maximum_temp NUMERIC,
                    pressure NUMERIC,
                    humidity NUMERIC,
                    wind_speed NUMERIC,
                    time_of_record TIMESTAMP,
                    sunrise_local_time TIMESTAMP,
                    sunset_local_time TIMESTAMP                    
                );
                '''
            )
    
        
            is_weather_api_ready = HttpSensor(         # waits for a specified response from an HTTP endpoint
            task_id = 'is_weather_api_ready',
            http_conn_id = 'weathermap_api',           # set it up in Airflow Connections
            endpoint = "/data/2.5/weather?q=houston&appid=271033f487513759d5309a8580d79501"
            )

       
            extract_weather_data = SimpleHttpOperator(
            task_id = 'extract_weather_data',
            http_conn_id = 'weathermap_api',
            endpoint = "/data/2.5/weather?q=houston&appid=271033f487513759d5309a8580d79501",
            method = 'GET',
            response_filter = lambda r: json.loads(r.text),
            log_response=True
            )

        
            transform_load_weather_data = PythonOperator(
            task_id = 'transform_load_weather_data',
            python_callable = transform_load_data           # transform_load_data is a function that we want to run for this task
            )


            load_weather_data_to_postgres = PythonOperator(
            task_id = "load_weather_data_to_postgres",
            python_callable = load_weather
            )


        
            create_table_1 >> truncate_table >> upload_S3_to_postgres
            create_table_2 >> is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> load_weather_data_to_postgres
        start_pipeline >> group_a >> join_data >> load_joined_data >> end_pipeline

import json
from datetime import datetime
import pandas as pd
import requests 


city_name = "Santa Cruz de Tenerife"
base_url = "https://api.openweathermap.org/data/2.5/weather?q="

with open("credentials.txt", "r") as f:
    api_key = f.read()


full_url = base_url + city_name + "&appid=" + api_key   

#EXTRACT
def etl_weather_data(full_url):
    r = requests.get(full_url)
    data = r.json()
    # print(data)


# TRANSFORM
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
    transformed_data = {"City": city,
                            "Description": weather_description,
                            "Temperature (C)": temp,
                            "Feels Like (C)": feels_like,
                            "Minimun Temp (C)":min_temp,
                            "Maximum Temp (C)": max_temp,
                            "Pressure": pressure,
                            "Humidty": humidity,
                            "Wind Speed": wind_speed,
                            "Time of Record": time_of_record,
                            "Sunrise (Local Time)":sunrise_time,
                            "Sunset (Local Time)": sunset_time                        
                            }
    #print(transformed_data)


    # convert the transformed data into a list
    transformed_data_list = [transformed_data]

    # transform the list into a data frame
    df_data = pd.DataFrame(transformed_data_list)
    print(df_data)

    # Now the data is ready to be loaded into a file or a database

    #LOAD
    df_data.to_csv("current_weather_santa_cruz_de_tenerife", index=False)


if __name__ == '__main__':
    etl_weather_data(full_url)
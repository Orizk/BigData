import requests
import json
from kafka import KafkaProducer
import time


producer = KafkaProducer(bootstrap_servers='192.168.33.13:9092')

current_temperature = hourly_temp = hourly_time = ""
lastresult = []
with open('french_cities.csv') as file:
    while line := file.readline():
        l = line.split(';')
        gps = str(l[4]).rstrip().split(", ")
        url ="https://api.open-meteo.com/v1/forecast?latitude="+gps[0]+"&longitude="+gps[1]+"&hourly=temperature_2m&current_weather=true&timezone=Europe%2FBerlin"
        data = json.loads(requests.get(url).content)
        i =0
        for k in data:
            if(k == "current_weather"):
                current_temperature = str(data[k]["temperature"])
            if(k == "hourly"):
                for val in data[k]["temperature_2m"]: 
                    hourly_temp = str(val)
                    hourly_time = str(data[k]["time"][i]).split("T")
                    final_hourly_time = hourly_time[0]+" "+hourly_time[1]
                    i = i+1
                    lastresult = l[1]+","+"FR-"+l[2]+","+current_temperature+","+hourly_temp+","+final_hourly_time
                    print(lastresult)
                    producer.send('weather', lastresult.rstrip().encode('utf-8'))
        time.sleep(1)

import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='192.168.33.13:9092')
with open('data_covid.csv') as file:
    while line := file.readline(): 
        producer.send('covid1', line.rstrip().encode('utf-8'))
        time.sleep(1)
        

    

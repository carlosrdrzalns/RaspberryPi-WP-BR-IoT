import time
import paho.mqtt.client as mqtt
import pandas as pd
import json
import pickle as pk
import datetime
import uuid
import random
import math
import threading
import time

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print("Failed to connect, return code: ", rc)

def on_publish(client, userdata, mid):
    print("Message published")

def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT broker")

def sendData(client, df, index, length):
    try:
        
        if(index > length):
            index = 0
        #Prepare object to be send
        
        obj = {
        }
        df.std()
        rnd = random.random()
        for i in range(1,len(df.columns)):
            value  = df.iloc[index][df.columns[i]] + rnd*df.std()[df.columns[i]]/10
            if math.isnan(value): value = 0
            obj[df.columns[i]] = value

        # Publish the data to a specific Adafruit IO feed
        client.publish('crodrigueza/feeds/pump-data.pump-data', json.dumps(obj))
        index += 1
         # Wait for 2 seconds before publishing the next set of data
    except Exception as e:
        print('Error:', e)


aio_username = 'crodrigueza'
aio_key = 'aio_BqmP55u5MbSH6xGzkBgJgymZiSib'
client = mqtt.Client(client_id=str(uuid.uuid4()))
client.username_pw_set(username=aio_username, password=aio_key)
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect


client.connect('io.adafruit.com', 1883)
df = pd.read_csv("sensor.csv")
df.drop(['timestamp'], inplace = True, axis =1)
df.drop(['machine_status'], inplace = True, axis =1)
length = len(df.index)

def repeated_execution(index):
    start_time = time.time()
    sendData(client=client, df=df, index=index, length=length)
    end_time = time.time()
    execution_time = end_time-start_time
    index=index+1
    threading.Timer(30-execution_time, repeated_execution, args=[index+1]).start()

repeated_execution(index=1)


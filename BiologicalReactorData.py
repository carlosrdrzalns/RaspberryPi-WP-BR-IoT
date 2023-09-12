import time
import paho.mqtt.client as mqtt
import pandas as pd
import json
import pickle as pk
import datetime
import uuid
import random

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print("Failed to connect, return code: ", rc)

def on_publish(client, userdata, mid):
    print("Message published")

def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT broker")

aio_username = ADAFRUIT_USERNAME
aio_key = ADAFRUIT_AIO_KEY
client = mqtt.Client(client_id=str(uuid.uuid4()))
client.username_pw_set(username=aio_username, password=aio_key)
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect

intVector = [31.6, 98.33,65,166.66,28,0,0,0,0.83,23.33,7.66,11.66,7.33];
intDeviation = [7.6376, 27.538, 37.7492, 76.3736, 5, 0, 0, 0, 0.2887, 11.54, 2.516, 2.8868, 1.5275]

client.connect('io.adafruit.com', 1883)
df1 = pd.read_csv("firstTank.csv")
df2 = pd.read_csv("secondTank.csv")
length = len(df1.index)
index = 0
while True:
    try:
        if(index > length):
            index = 0
        #Prepare object to be send
        rnd = random.random()
        
        obj = {
        }
        for i in range(len(df1.columns)-1):
            obj[df1.columns[i] + '0'] = intVector[i]+rnd*intDeviation[i] +random.random()*intDeviation[i]/10

        for i in range(len(df1.columns)-1):
            obj[df1.columns[i] + '_ini'] = df1.iloc[index][df1.columns[i]]+rnd*df1.std()[df1.columns[i]]/10


        for i in range(len(df2.columns)):
            obj[df2.columns[i] + '_ini2'] = df2.iloc[index][df2.columns[i]]+rnd*df1.std()[df1.columns[i]]/10

        # Publish the data to a specific Adafruit IO feed
        client.publish('ADAFRUIT_USERNAME/feeds/ADAFRUIT_BROKER', json.dumps(obj))
        index += 1
        time.sleep(10)  # Wait for 2 seconds before publishing the next set of data

    except Exception as e:
        print('Error:', e)
        continue

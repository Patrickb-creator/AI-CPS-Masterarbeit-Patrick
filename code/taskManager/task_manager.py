import paho.mqtt.client as mqtt
import os
import general_code.network_helpers as nw_helper

global MQTT_Publish_Topic, MQTT_Result_Topic, MQTT_Username, MQTT_Password

MQTT_Publish_Topic = "mqttTester"
MQTT_Result_Topic = "mqttTester/results"
MQTT_Username = "user1"
MQTT_Password = "WhHe1NPfDBJ%"

# TODO
def on_connect(client, userdata, flags, rc):
   print("Connected with result code "+ str(rc))

   # Subscribing in on_connect() means that if we lose the connection and
   # reconnect then subscriptions will be renewed.
   client.subscribe(MQTT_Publish_Topic, qos = 0)  # channel to deal with CoNM
   client.subscribe(MQTT_Result_Topic, qos = 0)

def on_message():
   return 0
   

if __name__ == '__main__':
   # specify client for messaging
   client = mqtt.Client()
   client.on_connect = on_connect
   client.on_message = on_message
   client.username_pw_set(username=MQTT_Username, password=MQTT_Password)

   # get broker ip
   MQTT_Broker = nw_helper.get_broker_ip()
   Broker_Port = 1883 

   # oder
   # MQTT_Broker = "localhost"
   
   # DB
   # MQTT_Broker = "172.18.230.56"

   # Raspi zu hause
   # MQTT_Broker = "192.168.178.53"

   # establish connection of client and server
   # - Method 1 - connect via plain MQTT protocol but with username and password
   client.connect(MQTT_Broker, Broker_Port)
   # client.connect(MQTT_Broker, Broker_Port, 60)
   # - Method 2 - connect via secure MQTT over TLS/SSL
   # TBD when required
   # - Method 3 - connect via MQTT over TLS/SSL with certificates
   # TBD when required
   # - Method 4 - connect via plain WebSockets configuration
   # TBD when required
   # - Method 5 - connect via WebSockets over TLS/SSL
   # TBD when required

   # Blocking call that processes network traffic, dispatches callbacks and
   # handles reconnecting.
   # Other loop*() functions are available that give a threaded interface and a
   # manual interface.

   # specify topics for subscriptions
   # 1. in cmd mosquitto_pub -h localhost -t "mqttTester" -m "Huhu"
   # 2. in cmd mosquitto_sub -h localhost -t "mqttTester"
   # 3. Code starten
   client_name = "TaskManager"

   # send connection Message to all connected PCs on Executing and Results channel
   client.publish(MQTT_Publish_Topic, 'This is the Manager. My name is '+ client_name +' and I have subscribed to topic '+ MQTT_Publish_Topic +'.')
   client.publish(MQTT_Result_Topic, 'This is the Manager. My name is '+ client_name +' and I have subscribed to topic '+ MQTT_Result_Topic +'.')
   # ...

   # start listening here
   client.loop_forever()
import paho.mqtt.client as mqtt
import os
import re
import sys
import time
import threading

global MQTT_Publish_Topic, MQTT_Result_Topic, MQTT_Username, MQTT_Password

MQTT_Publish_Topic = "mqttTester"
MQTT_Result_Topic = "mqttTester/results"
MQTT_Username = "user1"
MQTT_Password = "WhHe1NPfDBJ%"
connected_pc_list = set()  # using a unique entry set so every pc is only there once
ping_event = threading.Event()  # event to control the ping-threads
stop_event = threading.Event()

# get the latest broker ip of the broker which was started
def get_broker_ip():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    broker_dir = os.path.join(parent_dir, "messageBroker")
    ip_file = os.path.join(broker_dir, "broker_ip_log.txt")

    try:
        with open(ip_file, 'r', encoding='utf-8') as file:
            broker_ip = file.read()
        return broker_ip
    except FileNotFoundError:
        print(f"File {ip_file} not found")

# send pings to clients
def send_ping(client):
    while not stop_event.is_set():
      ping_event.wait() # wait until event starts
      client.publish("ping/request", "Ping from TaskManager", qos=1)
      time.sleep(10)

# monitor active clients
def monitor_clients():
   global ping_event

   while not stop_event.is_set():
      print(f"Active clients: {connected_pc_list}")
      if connected_pc_list and not ping_event.is_set():
         print("Clients connected. Resuming ping...")
         ping_event.set()  # acitvate ping
      elif not connected_pc_list and ping_event.is_set():
         print("No clients connected. Pausing ping...")
         ping_event.clear()  # pause ping
      time.sleep(10)

# callback function for mqtt connection
def on_connect(client, userdata, flags, rc):
   print("Connected with result code " + str(rc))
   client.subscribe(MQTT_Publish_Topic, qos=0)  # channel to deal with CoNM
   client.subscribe(MQTT_Result_Topic, qos=0)
   client.subscribe("status/#")  # Subscribe to the status of all clients
   client.subscribe("ping/response/#")  # Listen for ping responses

# callback when receiving messages
def on_message(client, userdata, msg):
   message = msg.payload.decode()
   topic = msg.topic

   print(f"Message received on {msg.topic}: {message}")

   # check for status messages
   if topic.startswith("status/"):
      client_name = topic.split("/")[1]
      if "Disconnected" in message:
         connected_pc_list.discard(client_name)
      elif "Connected" in message:
         connected_pc_list.add(client_name)

   # check for ping answers
   elif topic.startswith("ping/response/"):
      client_name = topic.split("/")[-1]
      connected_pc_list.add(client_name)

   # extract clients which introduce themselves
   match = re.search(r"My name is (\w+)", message)
   if match:
      connected_pc = match.group(1)
      connected_pc_list.add(connected_pc)

if __name__ == '__main__':

   """Init the TM, 
   Connecting to broker 
   and pub/sub to relevant tasks"""
   client = mqtt.Client()
   client_name = "TaskManager"
   
   client.on_connect = on_connect
   client.on_message = on_message
   client.username_pw_set(username=MQTT_Username, password=MQTT_Password)

   # set Last Will Message so the manager knows where not to give tasks anymore
   client.will_set(f"status/{client_name}", "Disconnected", qos=1, retain=True)

   # get broker ip
   MQTT_Broker = get_broker_ip()
   Broker_Port = 1883

   try: 
      # connect to MQTT broker
      try:
         client.connect(MQTT_Broker, Broker_Port)
      except Exception as e:
         print(f"Failed to connect to broker: {e}")
         sys.exit(1)

      # start ping and monitoring thread
      try:
         ping_thread = threading.Thread(target=send_ping, args=(client,))
         monitor_thread = threading.Thread(target=monitor_clients)
         ping_thread.start()
         monitor_thread.start()
      except Exception as e:
         print(f"Error starting threads: {e}")
         sys.exit(1)

      # publish initial messages
      client.publish(MQTT_Publish_Topic, f"This is the Manager. My name is {client_name} and I have subscribed to topic {MQTT_Publish_Topic}.")
      client.publish(MQTT_Result_Topic, f"This is the Manager. My name is {client_name} and I have subscribed to topic {MQTT_Result_Topic}.")

      """Getting Tasks and publishing them to random connected clients"""

      # start the MQTT client loop
      client.loop_forever()

   except KeyboardInterrupt:
      print("Keyboard interrupt detected. Exiting gracefully...")
   
   finally:
      ping_event.set()
      stop_event.set()
      print("Set ping_event and stop_event to False.")
      client.loop_stop()
      print("Stopped client loop.")
      client.disconnect()
      print("Client disconnected")
      # cancel all threads correctly
      ping_thread.join(timeout=5)
      monitor_thread.join(timeout=5)
      sys.exit(0)

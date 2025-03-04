"""
A little script to serve as communication client within the CoNM environment.
When requested, it initiates the corresponding AI activation via docker compose files.
Copyright (c) 2022 Marcus Grum
"""

__author__ = 'Marcus Grum, marcus.grum@uni-potsdam.de'

# with friendly permissions by Marcus Grum:
__thesis_author__ = 'Lena Siegmund, siegmund2@uni-potsdam.de'

# SPDX-License-Identifier: AGPL-3.0-or-later or individual license
# SPDX-FileCopyrightText: 2022 Marcus Grum <marcus.grum@uni-potsdam.de>

import subprocess
import paho.mqtt.client as mqtt
from multiprocessing import Process, Queue, current_process, freeze_support
import time
import csv
import os
import platform
import numpy
import socket
import random
import realize_scenarios_green as executor
import power_scheme_handler as ps_handler
import threading
import uuid
from queue import Queue
from datetime import datetime
import sys
from zeroconf import ServiceBrowser, Zeroconf, ServiceListener
import mqtt_broker_listener as broker_listener

# import experiments
import sys
sys.path.insert(0, '../experiments')

task_queue = Queue()

# specify global variables, so that they are known (1) at messageClient start and (2) at function calls from external scripts
global hostName, hostArch, log_directory
# hostName = os.name
# hostname muss dynamisch auf die Nummer 
# des PCs im Netzwerk zugewiesen werden oder wir gehen dann über die IP Adressen 
# die mit dem MQTT CLient verbunden sind
pc_name = "LenasPC"

# get MAC-address
mac_address = ':'.join(format(x, '02x') for x in uuid.getnode().to_bytes(6, 'big'))

hostArch = platform.machine()

# Log directory for task results
project_root = os.getcwd()  # main dir

# adapt directory to windows or linux depending on where it is running 
log_directory = os.path.join(project_root, "code/messageClient/task_logs_green")
if not os.path.exists(log_directory):
    os.makedirs(log_directory, exist_ok=True)
print(f"Logs are saved here: {log_directory}")

nvidia_gpu = False

try:
    subprocess.check_output('nvidia-smi')
    print('Nvidia GPU detected!')
    nvidia_gpu = True
    hostArch = hostArch + "_gpu"
except Exception:
    print('No Nvidia GPU in system!')
    hostArch = hostArch + ""

hostArch = hostArch.lower()

if hostArch == 'amd64':
    hostArch = 'x86_64'
if hostArch == 'amd64_gpu':
    hostArch = 'x86_64_gpu'

MQTT_Topic_Execute = 'mqttTester'
MQTT_Topic_Results = 'mqttTester/results'

# Utility functions
def get_or_generate_client_id():
    id_file_name = "client_id.txt"
    current_dir = os.path.dirname(os.path.abspath(__file__))
    id_file = os.path.join(current_dir, id_file_name)

    if os.path.exists(id_file):
        # load id from file if it was already created
        with open(id_file, 'r') as file:
            client_id = file.read().strip()
    else:
        # generate a new id 
        # random number with 10 digits, so that no other client in the network gets this number
        # if you want it more complex you can use the mac address and hash it -> unique id
        client_id = str(random.randint(100000, 999999)) 
        with open(id_file, 'w') as file:
            file.write(client_id)
    
    return client_id

# get the local IP address of the device
def get_local_ip():
    try:
        # Connection to a non-existent address (to determine the network interface)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))  # 8.8.8.8 is a public DNS server from Google
            ip_address = s.getsockname()[0]  # get ip of the device
        return ip_address
    except Exception as e:
        print(f"Error retrieving the IP address: {e}")
        return None

# get local ip
local_ip = get_local_ip()
print(f"Local IP: {local_ip}")

# get the latest broker ip of the brokedr which was started
def get_broker_ip_via_file():
    # current folder in which the executing code is located
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # go to the “code” parent folder
    parent_dir = os.path.dirname(current_dir)

    # Construct the path to the “messageBroker” folder
    broker_dir = os.path.join(parent_dir, "messageBroker")

    # full path to the “broker_ip_log.txt” file
    ip_file = os.path.join(broker_dir, "broker_ip_log.txt")

    try:
        with open(ip_file, 'r', encoding='utf-8') as file:
            broker_ip = file.read()
            
        return broker_ip
    except FileNotFoundError:
       print(f"File {ip_file} not found")

# Callback für Pings
def on_ping_request(client, userdata, msg):
    # print(f"Ping received: {msg.payload.decode()}") -> this works perfectly fine but it spams the cmd
    client.publish(f"ping/response/{client_id}", "I'm alive!", qos=1)

def load_data_fromfile(path):
    """
    This functions loads csv data from the 'path' and returns it.
    Remember, the data returned needs to be reshaped because it is flat.
    E.g. by data.reshape((maxNumberOfExperiments, maxIterationsInPhase1+maxIterationsInPhase2+1, maxMachines*maxValidationSets*maxStreams, maxNumberOfKPIs))
    """

    data = numpy.fromfile(path,sep=',',dtype=float)

    return data

def save_data_tofile(numpyArray, path):
    """
    This functions saves the data of variable 'numpyArray to the 'path'.
    """

    numpyArray.tofile(path,sep=',',format='%10.5f')

def load_data_from_CsvFile(path):
    """
    This functions loads csv data from the 'path' and returns it.
    """
    data = []
    with open(path + '.csv', newline='') as csvfile:
        # alternative delimiters '\t', ';', alternative quotechars '"', '|'
        spamreader = csv.reader(csvfile, delimiter='\t', quotechar='"')
        for row in spamreader:
            data.append(row)

    return data

def save_data_to_CsvFile(listOfResults, path):
    """
    This functions saves the simulation results of variable 'listOfResults to the 'path'.
    """
    with open(path + '.csv', 'w', newline='') as myfile:
        wr = csv.writer(myfile, quoting=csv.QUOTE_ALL, delimiter='\t')
        for i in range(len(listOfResults)):
            wr.writerow(listOfResults[i])

# The callback for when the client receives a CONNACK response from the server.
# connection function -> client connects to the Broker 
def on_connect(client, userdata, flags, rc):
    # print(f"Connected with result code str(rc), MAC-Adresse: {mac_address}")
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MQTT_Topic_Execute, qos = 0)  # channel to deal with CoNM
    client.subscribe("ping/request")  # Subscribe to pings
    client.subscribe(f"tasks/{client_id}")
    client.message_callback_add("ping/request", on_ping_request)  # Callback for pings
    # client.publish("devices/mac", mac_address) 

# clear log directory when processing many tasks at once
def clear_log_directory(log_directory):
    """
    clears log before new tasks are executed
    """
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)  # create dir if it does´nt exist yet

    # delete all files in the logdirectory
    for filename in os.listdir(log_directory):
        file_path = os.path.join(log_directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
    print(f"Log-Verzeichnis {log_directory} wurde geleert.")

# Handle incoming messages and push them to the task queue
def on_message(client, userdata, msg):
    message = msg.payload.decode()
    topic = msg.topic
    # print(f"Received message on topic {topic}: {message}")

    if topic.startswith(f"tasks/{client_id}"):
        # split message by newline and enqueue each task separately
        task_list = message.strip().split("\n")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} {client_id}: I received {len(task_list)} new tasks.")
        clear_log_directory(log_directory) # clear all logs to avoid spam
        client.publish("result/status/" + client_id, 1)
        for task in task_list:
            if task.strip():  # Check if task is not empty
                task_queue.put((topic, task))

def task_worker():
    while True:
        topic, message = task_queue.get()
        if message == "STOP":
            print("Stopping task worker.")
            break

        scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver = unroll_message(message)
        
        # that could actually be removed, because 
        # every client can edit every task 
        # and the task manager should distribute the tasks via the topics
        if receiver == "X":
            # set the power scheme for windows clients to saving when operating
            # ps_handler.set_specific_power_scheme(scheme="save")
            executor.realize_scenario(
                log_directory, 
                MQTT_Topic_Results, 
                scenario, 
                knowledge_base, 
                activation_base, 
                code_base, 
                learning_base, 
                client, 
                sender, 
                receiver, 
                client_id, 
                hostArch,
                sub_process_method="sequential")
            client.publish(MQTT_Topic_Results, client_id + ': This is a result indication! I have processed the ann request.')
            print(f"Task {scenario} executed by {client_id}.")
        task_queue.task_done()

        if task_queue.empty():
            # ps_handler.set_specific_power_scheme("balanced") # set power scheme back to have all other processes run on the device normally
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            completion_message = f"[{timestamp}] I processed all tasks.\n"
            client.publish("result/status/" + client_id, 0)
            client.publish("finish/" + client_id, f"Finished {client_id}")
            print(completion_message)

def unroll_message(message):
    """
    This functions unrolls variables from message and returns them.
    """
    scenario = (message.partition("scenario=")[2]).partition(", knowledge_base=")[0]
    knowledge_base = (message.partition("knowledge_base=")[2]).partition(", activation_base=")[0]
    activation_base = (message.partition("activation_base=")[2]).partition(", code_base=")[0]
    code_base = (message.partition("code_base=")[2]).partition(", learning_base=")[0]
    learning_base = (message.partition("learning_base=")[2]).partition(", sender=")[0]
    sender = (message.partition("sender=")[2]).partition(", receiver=")[0]
    receiver = (message.partition("receiver=")[2]).partition(".")[0].rstrip('"')

    return scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver

if __name__ == '__main__':
    """
    This function initiates communication client
    and manages the corresponding AI requests.
    """
    # adding freeze_support() ensures smooth execution when packaged for distribution 
    # while using multiprocessing features
    freeze_support()

    # optionally input parameters from CLI to rename host
    if len(sys.argv) > 1 and sys.argv[1] != "":
    # The argument exists and is not empty
        print("Argument gefunden:", sys.argv[1])
        client_id = sys.argv[1]
    else:
    # No argument available or argument is empty
        print("Kein Argument gefunden oder Argument ist leer.")

    # MQTT_Username = "user1"
    # MQTT_Password = "WhHe1NPfDBJ%"

    # specify client for messaging
    client = mqtt.Client(client_id=mac_address)
    client.on_connect = on_connect
    client.on_message = on_message
    # client.username_pw_set(username=MQTT_Username, password=MQTT_Password)

    # client_id = mac_address
    client_id = get_or_generate_client_id()
    print(f"This is the client with id {client_id}.")

    # Set Last Will Message
    client.will_set(f"status/{client_id}", "Disconnected", qos=1, retain=False)

    # get the broker ip from the mDNS
    broker_info = broker_listener.discover_broker()
    
    if broker_info:
        MQTT_Broker, Broker_Port = broker_info
        print(f"Using broker: {MQTT_Broker}:{Broker_Port}")
    else:
        print("No MQTT broker discovered, using fallback IP.")
        MQTT_Broker = get_broker_ip_via_file() or "localhost"
        Broker_Port = 1883

    # establish connection of client and server
    # - Method 1 - connect via plain MQTT protocol
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

    # announce presence of client at server's topic-specific message channel
    client.publish(MQTT_Topic_Execute, 'Hi there! My name is '+ client_id +' and I have subscribed to topic '+ MQTT_Topic_Execute+'.')
     # Sende regelmäßige Status-Updates
    client.publish(f"status/{client_id}", "Connected", qos=1, retain=True)

        # Start the task worker thread
    task_worker_thread = threading.Thread(target=task_worker, daemon=True)
    task_worker_thread.start()

    # start listening here
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("Shutting down...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Cleaning up before exit...")
        client.publish(f"status/{client_id}", "Disconnected", qos=1, retain=False)
        client.disconnect()
        print("Client disconnected.")
    
        task_queue.put((None, "STOP"))
        if 'task_worker_thread' in locals() and task_worker_thread.is_alive():
            task_worker_thread.join(timeout=5)
    
        print("Thread joined. Exiting now.")
        sys.exit(0)
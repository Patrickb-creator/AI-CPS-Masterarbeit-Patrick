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
import realize_scenarios as executor
import threading
from queue import Queue
from datetime import datetime
import sys

# import experiments
import sys
sys.path.insert(0, '../experiments')

task_queue = Queue()

# specify global variables, so that they are known (1) at messageClient start and (2) at function calls from external scripts
global hostName, hostArch, logDirectory
# hostName = os.name
# hostname muss dynamisch auf die Nummer 
# des PCs im Netzwerk zugewiesen werden oder wir gehen dann über die IP Adressen 
# die mit dem MQTT CLient verbunden sind
pc_name = "LenasPC"
hostArch = platform.machine()

# Log directory for task results
logDirectory =  "./code/messageClient/task_logs" 
os.makedirs(logDirectory, exist_ok=True) 

try:
    subprocess.check_output('nvidia-smi')
    print('Nvidia GPU detected!')
    hostArch = hostArch + "_gpu"
except Exception:
    print('No Nvidia GPU in system!')
    hostArch = hostArch + ""
if not os.path.exists(logDirectory):
    os.makedirs(logDirectory)

hostArch = hostArch.lower()

if hostArch == 'amd64':
     hostArch = 'x86_64'
if hostArch == 'amd_64_gpu':
     hostArch = 'x86_64_gpu'

MQTT_Topic_Execute = 'mqttTester'
MQTT_Topic_Results = 'mqttTester/results'
MQTT_Tasks = "tasks/#"

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
        # Verbindung zu einer nicht existierenden Adresse (um das Netzwerkinterface zu bestimmen)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))  # 8.8.8.8 ist ein öffentlicher DNS-Server von Google
            ip_address = s.getsockname()[0]  # get ip of the device
        return ip_address
    except Exception as e:
        print(f"Fehler beim Abrufen der IP-Adresse: {e}")
        return None

# get local ip
local_ip = get_local_ip()
print(f"Lokale IP-Adresse: {local_ip}")

# get the latest broker ip of the broker which was started
def get_broker_ip():
    # Der aktuelle Ordner, in dem sich der ausführende Code befindet
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Gehe zum Überordner "code"
    parent_dir = os.path.dirname(current_dir)

    # Konstruktiere den Pfad zum Ordner "messageBroker"
    broker_dir = os.path.join(parent_dir, "messageBroker")

    # Der vollständige Pfad zur Datei "broker_ip_log.txt"
    ip_file = os.path.join(broker_dir, "broker_ip_log.txt")

    try:
        with open(ip_file, 'r', encoding='utf-8') as file:
            broker_ip = file.read()
            
        return broker_ip
    except FileNotFoundError:
       print(f"File {ip_file} not found")

# Callback für Pings
def on_ping_request(client, userdata, msg):
    print(f"Ping received: {msg.payload.decode()}")
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
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MQTT_Topic_Execute, qos = 0)  # channel to deal with CoNM
    client.subscribe(MQTT_Topic_Results, qos = 0)
    client.subscribe("ping/request")  # Subscribe zum Empfangen von Pings
    client.subscribe("tasks/#")
    client.message_callback_add("ping/request", on_ping_request)  # Callback hinzufügen
    # ...

# Handle incoming messages and push them to the task queue
# die task queue macht dann nur sinn wenn ein paket an aufgaben an den client verteilt wird und nicht nur eins
def on_message(client, userdata, msg):
    message = msg.payload.decode()
    topic = msg.topic
    print(f"Received message on topic {topic}: {message}")

    if topic.startswith(f"tasks/{client_id}"):
        print(f"{client_id}: New task received: {message}")
        task_queue.put((topic, message))

def task_worker():
    while True:
        topic, message = task_queue.get()
        if message == "STOP":
            print("Stopping task worker.")
            break
        scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver = unroll_message(message)
        
        if receiver == client_id:
            executor.realize_scenario(
                logDirectory, 
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
                sub_process_method="parallel")
            print(f"Task {message} executed by {client_id}.")
        task_queue.task_done()

# The callback for when a PUBLISH message is received from the server.
# def on_message(client, userdata, msg):
#     """
#     This function continuously receives messages from broker and starts scenario realization.
#     It can be called via the following CLI commands:
#         1. Initiate example apply_annSolution from remote (for image classification):
#         mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=apply_annSolution, knowledge_base=marcusgrum/knowledgebase_apple_banana_orange_pump_20, activation_base=marcusgrum/activationbase_apple_okay_01, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
#         2. Initiate example create_annSolution from remote (for image classification):
#         mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=create_annSolution, knowledge_base=-, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=marcusgrum/learningbase_apple_banana_orange_pump_02, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
#         3. Initiate example refine_annSolution from remote (for image classification):
#         mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=refine_annSolution, knowledge_base=marcusgrum/knowledgebase_apple_banana_orange_pump_01, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=marcusgrum/learningbase_apple_banana_orange_pump_02, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
#         4. Initiate example wire_annSolution from remote (for image classification):
#         mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=wire_annSolution, knowledge_base=-, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
#         5. Initiate example publish_annSolution from remote (for image classification):
#         mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=publish_annSolution, knowledge_base=-, activation_base=-, code_base=-, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
#         6. Initiate experiment realize_annExperiment from remote:
#         mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=realize_annExperiment, knowledge_base=-, activation_base=-, code_base=-, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
        
#         1b. Initiate example apply_annSolution_for_transportClassification from remote (for transport classification):
#         mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=apply_annSolution_for_transportClassification, knowledge_base=marcusgrum/knowledgebase_cps1_transport_system_01, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_transport_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
#     """

#     # provide variables as global so that these are known in this thread
#     # global hostName

#     # unroll messages
#     message = msg.payload.decode()
#     topic = msg.topic
#     print(msg.topic + " " + str(message))

#     scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver = unroll_message(str(message))

#     # hier noch einfügen, dass ich nur die topics annehme, die meine id drin haben
#     if topic.startswith(f"tasks/{client_id}"):
#         print(f"{client_id}: Neue Aufgabe erhalten: {message}")

#     if(receiver == client_id):
#         # realize scenario, such as create_annSolution / apply_annSolution / refine_annSolution / publish_annSolution #/ realize_annExperiment
#         executor.realize_scenario(
#             logDirectory, 
#             MQTT_Topic_Results, 
#             scenario, 
#             knowledge_base, 
#             activation_base, 
#             code_base, 
#             learning_base, 
#             client, 
#             sender, 
#             receiver, 
#             client_id, 
#             hostArch,
#             sub_process_method="parallel")
        
#         print('Message of ' + sender + ' has been initiated at ' + receiver + ' by ' + client_id + '(' + os.name + ') successfully!')

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
    and manages the corresponding AI reguests.
    """
    # adding freeze_support() ensures smooth execution when packaged for distribution 
    # while using multiprocessing features
    freeze_support()

    # optionally input parameters from CLI to rename host
    if len(sys.argv) > 1 and sys.argv[1] != "":
    # Das Argument existiert und ist nicht leer
        print("Argument gefunden:", sys.argv[1])
        client_id = sys.argv[1]
    else:
    # Kein Argument vorhanden oder Argument ist leer
        print("Kein Argument gefunden oder Argument ist leer.")

    MQTT_Username = "user1"
    MQTT_Password = "WhHe1NPfDBJ%"

    # specify client for messaging
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(username=MQTT_Username, password=MQTT_Password)

    client_id = get_or_generate_client_id()
    print(f"This is the client with id {client_id}.")

    # Set Last Will Message
    client.will_set(f"status/{client_id}", "Disconnected", qos=1, retain=True)

# Broker einkommentieren
    # MQTT_Broker = "test.mosquitto.org" # world wide network via public test server (communication can be seen by everyone)
    # MQTT_Broker = "broker.hivemq.com" # world wide network via public test server (communication can be seen by everyone)
    # MQTT_Broker = "iot.eclipse.org"   # world wide network via public test server (communication can be seen by everyone)
    # communication in local network (start server with /usr/local/sbin/mosquitto -c /usr/local/etc/mosquitto/mosquitto.conf )
    
    # set broker IP
    MQTT_Broker = get_broker_ip()
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
        client.loop_stop()
        print("Shutting down...")
        client.disconnect()
        print("Client disconnected.")
        
        task_queue.put((None, "STOP"))
        if 'task_worker_thread' in locals() and task_worker_thread.is_alive():
            task_worker_thread.join(timeout=5)

    print("Thread joined. Exiting now.")
    sys.exit(0)
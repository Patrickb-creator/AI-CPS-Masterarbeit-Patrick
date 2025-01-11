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
import realize_scenarios as executor

# import experiments
import sys
sys.path.insert(0, '../experiments')


# specify global variables, so that they are known (1) at messageClient start and (2) at function calls from external scripts
global hostName, hostArch, logDirectory
# hostName = os.name
# hostName = "LenasPC"
hostArch = platform.machine()
logDirectory = "./logs"  # = $PWD/logs
try:
    subprocess.check_output('nvidia-smi')
    print('Nvidia GPU detected!')
    hostArch = hostArch + "_gpu"
except Exception:
    print('No Nvidia GPU in system!')
    hostArch = hostArch + ""
if not os.path.exists(logDirectory):
    os.makedirs(logDirectory)

MQTT_Topic = 'mqttTester'

# lokale IP Adresse des Geräts herausfinden, damit man es nicht immer selber im Code festlegen muss
def get_local_ip():
    try:
        # Verbindung zu einer nicht existierenden Adresse (um das Netzwerkinterface zu bestimmen)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))  # 8.8.8.8 ist ein öffentlicher DNS-Server von Google
            ip_address = s.getsockname()[0]  # Hol dir die IP-Adresse des Geräts
        return ip_address
    except Exception as e:
        print(f"Fehler beim Abrufen der IP-Adresse: {e}")
        return None

# IP-Adresse abrufen
local_ip = get_local_ip()
print(f"Lokale IP-Adresse: {local_ip}")

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
    client.subscribe(MQTT_Topic, qos=0)  # channel to deal with CoNM
    # ...

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
     """
     This function continuously receives messages from broker and starts scenario realization.
     It can be called via the following CLI commands:
          1. Initiate example apply_annSolution from remote (for image classification):
          mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=apply_annSolution, knowledge_base=marcusgrum/knowledgebase_apple_banana_orange_pump_20, activation_base=marcusgrum/activationbase_apple_okay_01, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          2. Initiate example create_annSolution from remote (for image classification):
          mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=create_annSolution, knowledge_base=-, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=marcusgrum/learningbase_apple_banana_orange_pump_02, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          3. Initiate example refine_annSolution from remote (for image classification):
          mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=refine_annSolution, knowledge_base=marcusgrum/knowledgebase_apple_banana_orange_pump_01, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=marcusgrum/learningbase_apple_banana_orange_pump_02, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          4. Initiate example wire_annSolution from remote (for image classification):
          mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=wire_annSolution, knowledge_base=-, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          5. Initiate example publish_annSolution from remote (for image classification):
          mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=publish_annSolution, knowledge_base=-, activation_base=-, code_base=-, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          6. Initiate experiment realize_annExperiment from remote:
          mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=realize_annExperiment, knowledge_base=-, activation_base=-, code_base=-, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          
          1b. Initiate example apply_annSolution_for_transportClassification from remote (for transport classification):
          mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=apply_annSolution_for_transportClassification, knowledge_base=marcusgrum/knowledgebase_cps1_transport_system_01, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_transport_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
     """

     # provide variables as global so that these are known in this thread
     # global hostName

     # unroll messages
     message = msg.payload.decode()
     topic = msg.topic
     print(msg.topic + " " + str(message))
     scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver = unroll_message(str(message))

     if(receiver == hostName):
          # realize scenario, such as create_annSolution / apply_annSolution / refine_annSolution / publish_annSolution #/ realize_annExperiment
          executor.realize_scenario(
              logDirectory, 
              MQTT_Topic, 
              scenario, 
              knowledge_base, 
              activation_base, 
              code_base, 
              learning_base, 
              client, 
              sender, 
              receiver, 
              hostName, 
              hostArch,
              sub_process_method="parallel")
          
          print('Message of ' + sender + ' has been initiated at ' + receiver + ' by ' + hostName + '(' + os.name + ') successfully!')

# new pub
def publish_answer():
     client.publish("response/topic", "Hello")

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
     receiver = (message.partition("receiver=")[2]).partition(".")[0]

     return scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver

if __name__ == '__main__':
     """
     This function initiates communication client
     and manages the corresponding AI reguests.
     Optional ToDo: 
     - Pull images for having most recent updates. 
     - Current code assumes images to be static (not changing over time).
     - If continual changes occure, a new AI case (and corresponding images) are released.
     """

# optionally input parameters from CLI to rename host
     if len(sys.argv) > 1 and sys.argv[1] != "":
        # Das Argument existiert und ist nicht leer
        print("Argument gefunden:", sys.argv[1])
        hostName = sys.argv[1]
     else:
     # Kein Argument vorhanden oder Argument ist leer
         print("Kein Argument gefunden oder Argument ist leer.")


     # specify client for messaging
     client = mqtt.Client()
     client.on_connect = on_connect
     client.on_message = on_message
     client.username_pw_set(username="user1", password="password1")

 # Broker einkommentieren
     # MQTT_Broker = "test.mosquitto.org" # world wide network via public test server (communication can be seen by everyone)
     # MQTT_Broker = "broker.hivemq.com" # world wide network via public test server (communication can be seen by everyone)
     # MQTT_Broker = "iot.eclipse.org"   # world wide network via public test server (communication can be seen by everyone)
     # communication in local network (start server with /usr/local/sbin/mosquitto -c /usr/local/etc/mosquitto/mosquitto.conf )
    # broker IP setzen

     # bei patrick
     # MQTT_Broker = "192.168.1.31"

     # zu hause mein eigener Laptop
     #MQTT_Broker = "192.168.178.21" 

     # Uni Griebnitzsee
     # MQTT_Broker = "10.15.18.169"

     # wenn der Broker auf dem selben Gerät läuft, wie auch gerade der Code, kann man die ip einfach über local ip senden, 
     # dazu muss aber glaub ich die Firewall unten sein sonst gibts einen Fehler
     # MQTT_Broker = local_ip

     # oder
     MQTT_Broker = "localhost"

     # Raspi zu hause
     # MQTT_Broker = "192.168.178.53"

     # establish connection of client and server
     # - Method 1 - connect via plain MQTT protocol
     client.connect(MQTT_Broker, 1883, 60)
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
     MQTT_Topic = 'mqttTester'
     # ...
     name = "LenasPC"

     # optionally announce presence of client at server's topic-specific message channel
     client.publish(MQTT_Topic, 'Hi there! My name is '+ name +' and I have subscribed to topic '+ MQTT_Topic+'.')
     # ...

     # start listening here
     client.loop_forever()
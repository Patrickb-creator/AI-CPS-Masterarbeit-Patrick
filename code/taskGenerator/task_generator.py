          #1. Initiate example apply_annSolution from remote (for image classification):
          #mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=apply_annSolution, knowledge_base=marcusgrum/knowledgebase_apple_banana_orange_pump_20, activation_base=marcusgrum/activationbase_apple_okay_01, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          #2. Initiate example create_annSolution from remote (for image classification):
         # mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=create_annSolution, knowledge_base=-, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=marcusgrum/learningbase_apple_banana_orange_pump_02, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          #3. Initiate example refine_annSolution from remote (for image classification):
         # mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=refine_annSolution, knowledge_base=marcusgrum/knowledgebase_apple_banana_orange_pump_01, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=marcusgrum/learningbase_apple_banana_orange_pump_02, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          #4. Initiate example wire_annSolution from remote (for image classification):
          #mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=wire_annSolution, knowledge_base=-, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          #5. Initiate example publish_annSolution from remote (for image classification):
          #mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=publish_annSolution, knowledge_base=-, activation_base=-, code_base=-, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
          #6. Initiate experiment realize_annExperiment from remote:
          #mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=realize_annExperiment, knowledge_base=-, activation_base=-, code_base=-, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883

import sys
import paho.mqtt.client as mqtt
import os
import random
import get_bases

def read_bases(bases_file):
    # Datei mit den Dateinamen einlesen
    with open(bases_file, "r") as file:
        file_entries = file.readlines()
            
        # Zeilen ohne Zeilenumbrüche bereinigen
        file_entries = [line.strip() for line in file_entries]
    
    return file_entries

def task_generator(number_of_tasks, MQTT_topic, sender, receiver, MQTT_Username="user1", MQTT_Password="WhHe1NPfDBJ%"):
    scenarios = [
        "apply_annSolution",
        "create_annSolution",
        "refine_annSolution",
        # "wire_annSolution",
        # "publish_annSolution",
        # "realize_annExperiment"
    ]

    # run the code to get the bases in the dir
    get_bases.get_bases()
    
    try:
        all_bases = read_bases("./code/taskGenerator/bases.txt")
        print("bases where found")
    except FileNotFoundError:
        print("The txt with the bases was not found")
        
    
    # bei manchen bases in manchen szenarien wird, wenn da nix rein kommen soll
    # einfach ein '-' gesetzt
    knowledgeBase = ["test"]
    activationBase = ["test"]
    codeBase= "marcusgrum/codebase_ai_core_for_image_classification"
    learningBase = ["test"]
    # receiver = random.randint(0,4)
    i = 1
    while i <= number_of_tasks:
        random_scenario = random.choice(scenarios)
        print(f"Zufälliger String: {random_scenario}")
        i+=1

    # die letzten zeilen erst durch TaskManager generieren???
    task = f"mosquitto_pub -t {MQTT_topic} " \
       f"-u {MQTT_Username} " \
       f"-P {MQTT_Password} " \
       f'-m "Please realize the following AI case: ' \
       f"scenario={scenarios[0]}, " \
       f"knowledge_base={knowledgeBase[0]}, " \
       f"activation_base={activationBase[0]}, " \
       f"code_base={codeBase}, " \
       f"learning_base={learningBase[0]}, " \
       f"sender={sender}, " \
       f"receiver={receiver}\" " \
       f"-h mqttTester " \
       f"-p 1883"
    
    print(task)


task_generator(10, "test", "test", "test")


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

# if you want to read bases from the bases.txt file, because you don´t have the images folder
# use this method in task_generator instead of the method in get_bases
def read_bases(bases_file):
    # read file with the basenames
    with open(bases_file, "r") as file:
        file_entries = file.readlines()
            
        # Zeilen ohne Zeilenumbrüche bereinigen
        file_entries = [line.strip() for line in file_entries]
    
    return file_entries

# apply und refine geht, create muss ich mir nochmal angucken
def task_generator(number_of_tasks, MQTT_topic, sender, receiver, host, MQTT_Username="user1", MQTT_Password="WhHe1NPfDBJ%",):
    scenarios = [
        "apply_annSolution",
        "create_annSolution",
        "refine_annSolution",
        # "wire_annSolution",
        # "publish_annSolution",
        # "realize_annExperiment"
    ]

    # get the bases in the dir
    allBases = get_bases.get_bases()
    
    # clustering der bases
    knowledgeBase = []
    activationBase = []
    codeBase = "marcusgrum/codebase_ai_core_for_image_classification"
    learningBase = []
    others = []

    for base in allBases:
        if "activationbase" in base:
            activationBase.append(base)
        elif "knowledgebase" in base:
            if "transport_system" in base:
                continue
            knowledgeBase.append(base)
        elif "learningbase" in base:
            learningBase.append(base)
        else:
            others.append(base)
    
    # receiver = random.randint(0,4)
    
    i = 0
    tasks = []

    # create n random task strings
    while i <= number_of_tasks:
        i+=1

        scenario = random.choice(scenarios)

        if scenario == "apply_annSolution":
            task = f"mosquitto_pub " \
                f"-h {host} " \
                f"-p 1883 " \
                f"-t \"{MQTT_topic}\" "\
                f"-u {MQTT_Username} " \
                f"-P {MQTT_Password} " \
                f'-m "Please realize the following AI case: ' \
                f"scenario={scenario}, " \
                f"knowledge_base={random.choice(knowledgeBase)}, " \
                f"activation_base={random.choice(activationBase)}, " \
                f"code_base={codeBase}, " \
                f"learning_base=-, " \
                f"sender={sender}, " \
                f"receiver={receiver}\" "
        elif scenario == "create_annSolution":
            task = f"mosquitto_pub " \
                f"-h {host} " \
                f"-p 1883 " \
                f"-t \"{MQTT_topic}\" "\
                f"-u {MQTT_Username} " \
                f"-P {MQTT_Password} " \
                f'-m "Please realize the following AI case: ' \
                f"scenario={scenario}, " \
                f"knowledge_base=-, " \
                f"activation_base=-, " \
                f"code_base={codeBase}, " \
                f"learning_base={random.choice(learningBase)}, " \
                f"sender={sender}, " \
                f"receiver={receiver}\" "
        elif scenario == "refine_annSolution":
            task = f"mosquitto_pub " \
                f"-h {host} " \
                f"-p 1883 " \
                f"-t \"{MQTT_topic}\" "\
                f"-u {MQTT_Username} " \
                f"-P {MQTT_Password} " \
                f'-m "Please realize the following AI case: ' \
                f"scenario={scenario}, " \
                f"knowledge_base={random.choice(knowledgeBase)}, " \
                f"activation_base=-, " \
                f"code_base={codeBase}, " \
                f"learning_base={random.choice(learningBase)}, " \
                f"sender={sender}, " \
                f"receiver={receiver}\" "

        tasks.append(task)

    # outputfile to store the generated tasks
    output_file = "./code/taskGenerator/generated_tasks.txt"

    # write in output file
    with open(output_file, "w") as file:
        for task in tasks:
            file.write(task + "\n")  # Jeder Eintrag in eine neue Zeile

    print(f"Tasks were stored in {output_file}.")

    # die letzten zeilen erst durch TaskManager generieren???


# example to run the task generator
task_generator(5, "mqttTester", "SenderA", "LenasPC", "localhost")


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
import time

# Add the parent directory (where "taskGenerator" is) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from messageClient import mqtt_broker_listener
from taskGenerator import get_bases

# MQTT-config
global MQTT_Publish_Topic, MQTT_Result_Topic, MQTT_Username, MQTT_Password, MQTT_Broker

MQTT_Port = 1883
MQTT_Username = "user1"
MQTT_Password = "WhHe1NPfDBJ%"
MQTT_Task_Generator_Topic = "task_generator"
MQTT_Broker = "localhost"

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

# if you want to read bases from the bases.txt file, because you don´t have the images folder
# use this method in task_generator instead of the method in get_bases
def read_bases(bases_file):
    # read file with the basenames
    with open(bases_file, "r") as file:
        file_entries = file.readlines()
            
        # Zeilen ohne Zeilenumbrüche bereinigen
        file_entries = [line.strip() for line in file_entries]
    
    return file_entries

# get the latest broker IP of the broker which was started
def get_broker_ip_via_file():
    broker_dir = os.path.join(parent_dir, "messageBroker")
    ip_file = os.path.join(broker_dir, "broker_ip_log.txt")
    try:
        with open(ip_file, 'r', encoding='utf-8') as file:
            broker_ip = file.read().strip()
        return broker_ip
    except FileNotFoundError:
        print(f"File {ip_file} not found")
        return "localhost"  # Fallback auf localhost

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe("tasks_done")

def on_message(client, userdata, msg):
    # global MQTT_Broker 
    message = msg.payload.decode()
    topic = msg.topic

    if topic == "tasks_done":
        # change num of tasks if you need more
        task_generator(number_of_tasks=10, MQTT_topic="mqttTester", host=MQTT_Broker, client=client)


def task_generator(number_of_tasks, MQTT_topic, host, client, MQTT_Username="user1", MQTT_Password="WhHe1NPfDBJ%",):
    # scenarios = [
    #     "apply_annSolution",
    #     "create_annSolution",
    #     "refine_annSolution",
    # ]

    # fet bases from the directory
    all_bases = get_bases.get_bases()

    # filter out specific knowledge bases
    excluded_bases = ["marcusgrum/knowledgebase_cps1_transport_system_01", "marcusgrum/knowledgebase_cps2_transport_system_01"]

    knowledge_base = [base for base in all_bases if "knowledgebase" in base and base not in excluded_bases]
    activation_base = [base for base in all_bases if "activationbase" in base]
    learning_base = [base for base in all_bases if "learningbase" in base]
    code_base = "marcusgrum/codebase_ai_core_for_image_classification"

#  for _ in range(number_of_tasks):
#         scenario = random.choice(scenarios)
#         task = {
#             "scenario": scenario,
#             "knowledge_base": random.choice(knowledge_base) if scenario != "create_annSolution" else "-",
#             "activation_base": random.choice(activation_base) if scenario != "create_annSolution" else "-",
#             "learning_base": random.choice(learning_base) if scenario != "apply_annSolution" else "-",
#             "code_base": code_base,
#             "sender": "{SENDER}",  # Platzhalter
#             "receiver": "{RECEIVER}",  # Platzhalter
#         }
#         tasks.append(task)

    tasks = []
    for _ in range(number_of_tasks):
        # scenario = random.choice(scenarios)
        scenario = "apply_annSolution"
        # if scenario == "apply_annSolution":
        task = f"mosquitto_pub " \
            f"-h {host} " \
            f"-p 1883 " \
            f"-t \"{MQTT_topic}\" "\
            f"-u {MQTT_Username} " \
            f"-P {MQTT_Password} " \
            f'-m "Please realize the following AI case: ' \
            f"scenario={scenario}, " \
            f"knowledge_base={random.choice(knowledge_base)}, " \
            f"activation_base={random.choice(activation_base)}, " \
            f"code_base={code_base}, " \
            f"learning_base=-, " #\
            # f"sender={sender}, " \
            # f"receiver={receiver}\" "
        # elif scenario == "create_annSolution":
        #     task = f"mosquitto_pub " \
        #         f"-h {host} " \
        #         f"-p 1883 " \
        #         f"-t \"{MQTT_topic}\" "\
        #         f"-u {MQTT_Username} " \
        #         f"-P {MQTT_Password} " \
        #         f'-m "Please realize the following AI case: ' \
        #         f"scenario={scenario}, " \
        #         f"knowledge_base=-, " \
        #         f"activation_base=-, " \
        #         f"code_base={code_base}, " \
        #         f"learning_base={random.choice(learning_base)}, " #\
        #         # f"sender={sender}, " \
        #         # f"receiver={receiver}\" "
        # elif scenario == "refine_annSolution":
        #     task = f"mosquitto_pub " \
        #         f"-h {host} " \
        #         f"-p 1883 " \
        #         f"-t \"{MQTT_topic}\" "\
        #         f"-u {MQTT_Username} " \
        #         f"-P {MQTT_Password} " \
        #         f'-m "Please realize the following AI case: ' \
        #         f"scenario={scenario}, " \
        #         f"knowledge_base={random.choice(knowledge_base)}, " \
        #         f"activation_base=-, " \
        #         f"code_base={code_base}, " \
        #         f"learning_base={random.choice(learning_base)}, " # \
                # "sender={sender}, " \
                # f"receiver={receiver}\" "
        # elif scenario == "wire_annSolution":
        #     # mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=wire_annSolution, knowledge_base=-, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
        #     task = f"mosquitto_pub " \
        #             f"-h {host} " \
        #             f"-p 1883 " \
        #             f"-t \"{MQTT_topic}\" "\
        #             f"-u {MQTT_Username} " \
        #             f"-P {MQTT_Password} " \
        #             f'-m "Please realize the following AI case: ' \
        #             f"scenario={scenario}, " \
        #             f"knowledge_base=-, " \
        #             f"activation_base=-, " \
        #             f"code_base={code_base}, " \
        #             f"learning_base=-, " #\
        #             # f"sender={sender}, " \
        #             # f"receiver={receiver}\" "
        tasks.append(task)

    # store the generated tasks to an output file
    output_file = os.path.join(current_dir, "generated_tasks.txt")
    with open(output_file, "w") as file:
        for task in tasks:
            file.write(task + "\n")

    print(f"{number_of_tasks} tasks were stored in {output_file}.")

    # publih a message to the Task Manager, that new Tasks where generated
    client.publish(MQTT_Task_Generator_Topic, f"{number_of_tasks} new tasks generated", qos=1)
    print(f"Published task notification to topic '{MQTT_Task_Generator_Topic}'.")

def main():
    # initialize MQTT-Client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(MQTT_Username, MQTT_Password)

    # get the broker ip from the mDNS
    broker_info = mqtt_broker_listener.discover_broker()
    
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

    # for the first triggering the user has to start one tg by himself 
    # after that the tg is waiting for done messages from the task manager
    user_input = input("Start the Task Generating Process? (Enter 'y' to start or 'exit' to quit): ")
    if user_input.lower() == "y":
        print("Starting the task generating process")
        task_generator(10, "mqttTester", host=MQTT_Broker, client=client)
    elif user_input.lower() == "exit":
        client.disconnect()
        sys.exit(0)
    else:
        print("no correct input, try again.")

    client.loop_forever()

    # while True:
    #     try:
    #         user_input = input("How many tasks should be generated? (Enter a number or 'exit' to quit): ")
    #         if user_input.lower() == "exit":
    #             print("Exiting Task Generator.")
    #             break

    #         number_of_tasks = int(user_input)
    #         if number_of_tasks <= 0:
    #             print("Please enter a positive number.")
    #             continue

    #         task_generator(number_of_tasks=number_of_tasks, MQTT_topic="mqttTester", client=client, host=MQTT_Broker)
    #     except ValueError:
    #         print("Invalid input. Please enter a valid number.")

    # client.loop_stop()
    # client.disconnect()

if __name__ == "__main__":
    main()

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
MQTT_Port = 1883
MQTT_Username = "user1"
MQTT_Password = "WhHe1NPfDBJ%"
MQTT_Task_Generator_Topic = "task_generator"

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

def get_broker_ip_via_file():
    broker_dir = os.path.join(parent_dir, "messageBroker")
    ip_file = os.path.join(broker_dir, "broker_ip_log.txt")
    try:
        with open(ip_file, 'r', encoding='utf-8') as file:
            broker_ip = file.read().strip()
        return broker_ip
    except FileNotFoundError:
        print(f"File {ip_file} not found")
        return "localhost"

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")


# this is the task generator for reproduction of experiments! it always gives a weighted distribution of tasks 
# so we can compare the scenarios of e.g. 10 or 100 tasks :)
def task_generator(number_of_tasks, MQTT_topic, client, host):
    scenarios = [
        "apply_annSolution",
        "create_annSolution",
        "refine_annSolution",
        "wire_annSolution"
    ]
    weights = [0.5, 0.2, 0.25, 0.05]  # weighted possibilitys: apply 50%, create 20%, refine 25%, wire 5%

    # get bases
    all_bases = get_bases.get_bases()

    # filter bases
    excluded_bases = ["marcusgrum/knowledgebase_cps1_transport_system_01", "marcusgrum/knowledgebase_cps2_transport_system_01"]
    knowledge_base = [base for base in all_bases if "knowledgebase" in base and base not in excluded_bases]
    activation_base = [base for base in all_bases if "activationbase" in base]
    learning_base = [base for base in all_bases if "learningbase" in base]
    code_base = "marcusgrum/codebase_ai_core_for_image_classification"
    
    # Statistik-Tracking
    scenario_count = {scenario: 0 for scenario in scenarios}

    tasks = []
    for _ in range(number_of_tasks):
        scenario = random.choices(scenarios, weights=weights)[0]
        scenario_count[scenario] += 1
        if scenario == "apply_annSolution":
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
                f"code_base={code_base}, " \
                f"learning_base={random.choice(learning_base)}, " #\
                # f"sender={sender}, " \
                # f"receiver={receiver}\" "
        elif scenario == "refine_annSolution":
            task = f"mosquitto_pub " \
                f"-h {host} " \
                f"-p 1883 " \
                f"-t \"{MQTT_topic}\" "\
                f"-u {MQTT_Username} " \
                f"-P {MQTT_Password} " \
                f'-m "Please realize the following AI case: ' \
                f"scenario={scenario}, " \
                f"knowledge_base={random.choice(knowledge_base)}, " \
                f"activation_base=-, " \
                f"code_base={code_base}, " \
                f"learning_base={random.choice(learning_base)}, " # \
                # "sender={sender}, " \
                # f"receiver={receiver}\" "
        elif scenario == "wire_annSolution":
            # mosquitto_pub -t "CoNM/workflow_system" -u user1 -P password1 -m "Please realize the following AI case: scenario=wire_annSolution, knowledge_base=-, activation_base=-, code_base=marcusgrum/codebase_ai_core_for_image_classification, learning_base=-, sender=SenderA, receiver=ReceiverB." -h "test.mosquitto.org" -p 1883
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
                    f"code_base={code_base}, " \
                    f"learning_base=-, " #\
                    # f"sender={sender}, " \
                    # f"receiver={receiver}\" "
        tasks.append(task)
    
    print("Task Distribution:")
    for scenario, count in scenario_count.items():
        percentage = (count / number_of_tasks) * 100
        print(f"{scenario}: {count} tasks ({percentage:.2f}%)")
    
    # save generated task types
    output_file = os.path.join(current_dir, "weighted_equals_generated_tasks.txt")
    with open(output_file, "w") as file:
        for task in tasks:
            file.write(task + "\n")

    print(f"{number_of_tasks} tasks were stored in {output_file}.")

    # inform manager about new tasks
    client.publish(MQTT_Task_Generator_Topic, f"{number_of_tasks} new tasks generated", qos=1)
    print(f"Published task notification to topic '{MQTT_Task_Generator_Topic}'.")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.username_pw_set(MQTT_Username, MQTT_Password)
    broker_info = mqtt_broker_listener.discover_broker()
    
    if broker_info:
        MQTT_Broker, Broker_Port = broker_info
        print(f"Using broker: {MQTT_Broker}:{Broker_Port}")
    else:
        print("No MQTT broker discovered, using fallback IP.")
        MQTT_Broker = get_broker_ip_via_file() or "localhost"
        Broker_Port = 1883

    client.connect(MQTT_Broker, Broker_Port)
    client.loop_start()

    while True:
        try:
            user_input = input("How many tasks should be generated? (Enter a number or 'exit' to quit): ")
            if user_input.lower() == "exit":
                print("Exiting Task Generator.")
                break

            number_of_tasks = int(user_input)
            if number_of_tasks <= 0:
                print("Please enter a positive number.")
                continue

            task_generator(number_of_tasks, "mqttTester", client, MQTT_Broker)
        except ValueError:
            print("Invalid input. Please enter a valid number.")

    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    main()

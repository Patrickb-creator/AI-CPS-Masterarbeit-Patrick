import paho.mqtt.client as mqtt
import os
import re
import sys
import time
import threading
import random

# Add the parent directory (where "taskGenerator" is) to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

global MQTT_Publish_Topic, MQTT_Result_Topic, MQTT_Username, MQTT_Password

client_id = "TaskManager"
MQTT_Port = 1883
MQTT_Username = "user1"
MQTT_Password = "WhHe1NPfDBJ%"
MQTT_Publish_Topic = "mqttTester"
MQTT_Result_Topic = "mqttTester/results"
MQTT_Task_Generator_Topic = "task_generator"
connected_pc_list = set()  # Unique set of connected PCs
ping_event = threading.Event()  # Event to control ping threads
stop_event = threading.Event()
task_event = threading.Event()

# Global task list
task_list = []
task_lock = threading.Lock()  # Ensure thread-safe access to task_list

# get the latest broker ip of the broker which was started
def get_broker_ip():
    broker_dir = os.path.join(parent_dir, "messageBroker")
    ip_file = os.path.join(broker_dir, "broker_ip_log.txt")

    try:
        with open(ip_file, 'r', encoding='utf-8') as file:
            broker_ip = file.read().strip()
        return broker_ip
    except FileNotFoundError:
        print(f"File {ip_file} not found")

# Function to read tasks from file and populate task_list
def load_tasks_from_file():
    global task_list
    generator_dir = os.path.join(parent_dir, "taskGenerator")
    task_file = os.path.join(generator_dir, "generated_tasks.txt")

    try:
        with open(task_file, 'r', encoding='utf-8') as file:
            with task_lock:
                task_list = [line.strip() for line in file.readlines()]
        print(f"Loaded tasks: {task_list}")
    except FileNotFoundError:
        print(f"File {task_file} not found")

def distribute_tasks(client):
    global task_list
    while not stop_event.is_set():
        if not connected_pc_list:
            print("No connected clients. Waiting...")
            time.sleep(5)
            continue

        with task_lock:
            if not task_list:
                print("No tasks available. Waiting for new tasks...")
                time.sleep(5)
                continue

            # Get a random task and client
            task = task_list.pop(0)  # Get the first task
            target_client = random.choice(list(connected_pc_list))

        # Send task to the selected client
        client.publish(f"tasks/{target_client}", task, qos=1)
        print(f"Sent task to {target_client}: {task}")

        time.sleep(2)  # Small delay to avoid overwhelming clients

# send pings to clients
def send_ping(client):
    while not stop_event.is_set():
        ping_event.wait()  # Wait until event starts
        client.publish("ping/request", "Ping from TaskManager", qos=1)
        time.sleep(10)

def monitor_clients():
    global ping_event

    while not stop_event.is_set():
        print(f"Active clients: {connected_pc_list}")
        if connected_pc_list and not ping_event.is_set():
            print("Clients connected. Resuming ping...")
            ping_event.set()  # Activate ping
        elif not connected_pc_list and ping_event.is_set():
            print("No clients connected. Pausing ping...")
            ping_event.clear()  # Pause ping
        time.sleep(10)

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(MQTT_Publish_Topic, qos=0)
    client.subscribe(MQTT_Result_Topic, qos=0)
    client.subscribe("status/#")
    client.subscribe("ping/response/#")
    client.subscribe(MQTT_Task_Generator_Topic, qos=0)

def on_message(client, userdata, msg):
    global task_list
    message = msg.payload.decode()
    topic = msg.topic

    print(f"Message received on {msg.topic}: {message}")

    if topic.startswith("status/"):
        client_name = topic.split("/")[1]
        if "Disconnected" in message:
            connected_pc_list.discard(client_name)
        elif "Connected" in message:
            connected_pc_list.add(client_name)

    elif topic.startswith("ping/response/"):
        client_name = topic.split("/")[-1]
        connected_pc_list.add(client_name)

    elif topic.startswith("task_generator"):
        print("Task generator triggered. Loading tasks...")
        load_tasks_from_file()

    match = re.search(r"My name is (\w+)", message)
    if match:
        connected_pc = match.group(1)
        connected_pc_list.add(connected_pc)

if __name__ == '__main__':
    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(username=MQTT_Username, password=MQTT_Password)
    client.will_set(f"status/{client_id}", "Disconnected", qos=1, retain=True)

    MQTT_Broker = get_broker_ip()
    Broker_Port = 1883

    try:
        client.connect(MQTT_Broker, Broker_Port)

        ping_thread = threading.Thread(target=send_ping, args=(client,))
        monitor_thread = threading.Thread(target=monitor_clients)
        task_thread = threading.Thread(target=distribute_tasks, args=(client,))

        ping_thread.start()
        monitor_thread.start()
        task_thread.start()

        client.loop_forever()

    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Exiting gracefully...")

    finally:
        ping_event.set()
        stop_event.set()
        task_event.set()
        print("Set ping_event and stop_event to False.")
        client.loop_stop()
        print("Stopped client loop.")
        client.disconnect()
        print("Client disconnected.")

        # Join threads only if they are alive
        if 'ping_thread' in locals() and ping_thread.is_alive():
            ping_thread.join(timeout=5)
        if 'monitor_thread' in locals() and monitor_thread.is_alive():
            monitor_thread.join(timeout=5)
        if 'task_thread' in locals() and task_thread.is_alive():
            task_thread.join(timeout=5)

        print("Threads joined. Exiting now.")
        sys.exit(0)

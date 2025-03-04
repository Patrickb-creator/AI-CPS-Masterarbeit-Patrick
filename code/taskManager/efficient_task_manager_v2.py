"""
To efficiently allocate tasks based on historical aggregated energy data, we can use an approach that takes into account the relative energy efficiency of each client.

Steps to allocate tasks based on energy consumption
Calculate the energy efficiency:
For each client, determine the energy efficiency by dividing the number of tasks processed by the summed energy consumption. A higher efficiency means that the client has consumed less energy per task.

Sorting the clients:
Sort the clients based on their energy efficiency. The clients with the highest efficiency should be preferentially assigned tasks.

Distribution of tasks:
Distribute tasks to clients in order of energy efficiency until all tasks are assigned.

Monitoring and adjustment:
Monitor the status and energy consumption of clients during task distribution and adjust the assignment in real time when new data arrives or when clients finish their tasks.
"""


import paho.mqtt.client as mqtt
import os
import re
import sys
import time
import threading
import random
from collections import defaultdict
from wakeonlan import send_magic_packet
import json
import pandas as pd
import numpy as np

# Add the parent directory (where "taskGenerator" is) to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

global MQTT_Publish_Topic, MQTT_Result_Topic, MQTT_Username, MQTT_Password

client_name = "EfficientTaskManager"
MQTT_Port = 1883
MQTT_Username = "user1"
MQTT_Password = "WhHe1NPfDBJ%"
MQTT_Publish_Topic = "mqttTester"
MQTT_Result_Topic = "mqttTester/results"
MQTT_Task_Generator_Topic = "task_generator"
connected_clients = set()  # unique set of connected PCs

ping_event = threading.Event()  # event to control ping threads
stop_event = threading.Event()
task_event = threading.Event()

# message_count = 0  # counter for received messages on results topic
finisher_counter = 0  # counter for received messages on results topic
task_num = 0  # counter for loaded tasks

timestamp_file = None
log_lock = threading.Lock() # ensure logging 
write_to_power_log_lock = threading.Lock()

# global task list
task_list = []
task_lock = threading.Lock()  # ensure thread-safe access to task_list

client_status = defaultdict(int)  # 1: task distributed, 0: tasks completed

# cache for current measured values per client
power_tracking = defaultdict(list)  # Stores all measured power values per client
task_count = defaultdict(int)   # Stores how many tasks a client has received

# Cache for start and end time per Client and Batch
task_timing = defaultdict(list)  

# Create an empty DataFrame to store the consumption data
columns = [
    "client_id", 
    "total_power_usage", 
    "relevant_power_values",
    "num_of_power_values", # how many values were collected during the process
    "tasks_assigned", 
    "efficiency_per_task", # power in watt per task
    "efficiency", # inverted eff
    "total_duration", 
    "time_per_task"]

df_client_power = pd.DataFrame(columns=columns)

# Saving the sorted clients and their efficiencies.
df_client_efficiency = pd.DataFrame(columns=["iteration", "client_id", "efficiency_per_task"])

# get the latest broker ip of the broker which was started
def get_broker_ip_via_file():
    broker_dir = os.path.join(parent_dir, "messageBroker")
    ip_file = os.path.join(broker_dir, "broker_ip_log.txt")

    try:
        with open(ip_file, 'r', encoding='utf-8') as file:
            broker_ip = file.read().strip()
        return broker_ip
    except FileNotFoundError:
        print(f"File {ip_file} not found")

def assign_task(client_id):
   """Increases the task counter for a client"""
   task_count[client_id] += 1

def start_task_session(client_id):
   """Initializes a new measurement series for a client"""
   power_tracking[client_id] = []  # Empty list for measured current values
   task_count[client_id] = 0  # reset task number
   task_timing[client_id].append({"start_time": time.time()})  # save start time

def record_power_usage(client_id, power_value):
   """Saves individual power consumption values during processing"""
   if client_id in task_timing and task_timing[client_id]:  # start task
      power_tracking[client_id].append((time.time(), power_value))  # save time stamps

def end_task_session(client_id, end_time):
    """Called when a client reports that it is ready"""
    global df_client_power

    if client_id not in power_tracking:
       print(f"⚠️ No Power-Tracking for {client_id} found!")
       return

    start_time = task_timing[client_id][-1]["start_time"]  # get start time

    # just for debugging
    local_time = time.localtime(start_time)
    formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    local_time_2 = time.localtime(end_time)
    formatted_time_2 = time.strftime("%Y-%m-%d %H:%M:%S", local_time_2)

    # Only add up values within the time window
    relevant_power_values = [
       power for timestamp, power in power_tracking[client_id] 
       if start_time <= timestamp <= end_time
    ]

    total_power = sum(relevant_power_values) # Sum only relevant values
    total_tasks = task_count.get(client_id, 0)  
    inv_efficiency = total_tasks / total_power if total_power > 0 else 0  
    efficiency_per_task = total_power / total_tasks

    # is in seconds because time is in epoch, this Unix timestamp
    total_duration = end_time - start_time  
    time_per_task = total_duration / total_tasks if total_tasks > 0 else 0  
    
    # add new data to dataframe
    new_data = pd.DataFrame([{
       "client_id": client_id,
       "total_power_usage": total_power,
       "relevant_power_values": relevant_power_values,
       "num_of_power_values": len(relevant_power_values),
       "tasks_assigned": total_tasks,
       "efficiency_per_task": efficiency_per_task,
       "efficiency": inv_efficiency,
       "total_duration": total_duration,
       "time_per_task": time_per_task
    }])

    df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)

    print(f"✅ Save Data for {client_id}: {new_data.to_dict(orient='records')}")

    # Empty memory for the next measurement
    del power_tracking[client_id]
    del task_count[client_id]


def aggregate_last_n_entries(n=5):
    """
    Aggregates the last `n` entries of a client and creates a new line with summed and averaged values.
    
    Parameters:
       n (int): The number of recent entries to be used. it should correspond to the number of connected clients
    
    Returns:
       pd.DataFrame: A DataFrame with the aggregated new row.
    """
    global df_client_power

    # Select the last `n` lines for the specified client
    # last_n_entries = df_client_power[df_client_power["client_id"] == client_id].tail(n)
    last_n_entries = df_client_power.tail(n)

    if last_n_entries.empty:
       # print(f"Found no last {n} entries for client {client_id}.")
       print("No last entries found.")
       return None  # return empty dataframe row

    # calculate sums and means of the values
    total_power = last_n_entries["total_power_usage"].sum()
    total_tasks = last_n_entries["tasks_assigned"].sum()
    total_duration = last_n_entries["total_duration"].sum()
    total_power_values = last_n_entries["num_of_power_values"].sum()
    
    avg_efficiency_per_task = last_n_entries["efficiency_per_task"].mean()
    avg_inv_efficiency = last_n_entries["efficiency"].mean()
    avg_time_per_task = last_n_entries["time_per_task"].mean()
    # avg_power_values = last_n_entries["num_of_power_values"].mean()

    # create new df row with aggregated data
    new_data = pd.DataFrame([{
       "client_id": 0, # for all clients
       "total_power_usage": total_power,
       "relevant_power_values": [], # could be all values for every client together but i think thats not relevant
       "num_of_power_values": total_power_values,
       "tasks_assigned": total_tasks,
       "efficiency_per_task": avg_efficiency_per_task, 
       "efficiency": avg_inv_efficiency,
       "total_duration": total_duration,
       "time_per_task": avg_time_per_task
    }])

    df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)

# read tasks from file and populate task_listt
def load_tasks_from_file():
    global task_list
    global task_count
    global task_num
    global timestamp_file
    global finisher_counter

    finisher_counter = 0

    generator_dir = os.path.join(parent_dir, "taskGenerator")
    manager_dir = os.path.join(parent_dir, "taskManager")

    # log dir for logs of the TM in regards of processing and distributing the tasks
    log_directory = os.path.join(manager_dir, 'logs')
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # change this if you use different generators!!
    task_file = os.path.join(generator_dir, "generated_tasks.txt")

    try:
        with open(task_file, 'r', encoding='utf-8') as file:
            with task_lock:
                # Load tasks from file that was generated by task_generator
                loaded_tasks = [line.strip() for line in file.readlines() if line.strip()]

                # Extend every task with sender and receiver
                task_list = [
                    f"{task} sender={client_name}, receiver=X\""
                    for task in loaded_tasks
                ]
        print(f"Loaded tasks: {task_list}")
        task_num = len(loaded_tasks)
    except Exception as e:
        print(f"File {task_file} not found Error loading tasks:{e}.")

def distribute_tasks_randomly():
    global task_list 
    global connected_clients

    # random.shuffle(task_list)  # Aufgaben zufällig mischen
    split_indices = sorted(random.sample(range(1, len(task_list)), len(connected_clients) - 1))  # Zufällige Trennstellen setzen
    sublists = [task_list[i:j] for i, j in zip([0] + split_indices, split_indices + [None])]
    
    client_task_dict = {client: tasks for client, tasks in zip(connected_clients, sublists)}
    return {client: tasks for client, tasks in zip(connected_clients, sublists)}

def calculate_average_efficiency():
    global df_client_power

    if df_client_power.empty:
        return {}

    # calculation of the average per client
    avg_efficiency = df_client_power.groupby("client_id").agg(
        total_tasks=("tasks_assigned", "sum"),
        total_power=("total_power_usage", "sum")
    )

    # efficiency = power per task in W
    avg_efficiency["efficiency"] = avg_efficiency["total_tasks"] / avg_efficiency["total_power"]
    
    # convert to dictionary that can then be used for intelligent task distribution
    return avg_efficiency["efficiency"].to_dict()

# TODO: einbauen dass was in sleep gesetzt wird?
def distribute_tasks_by_efficiency():
    """
    distributes tasks based on historical energy efficiency.

    Returns: Dict like: {“Client_A”: [tasks], “Client_B”: [tasks], ...}

    The function distributes tasks based on the efficiency of each client. 
    More efficient clients receive more tasks. 
    The distribution is random, but takes into account the efficiency of each client, 
    where this is determined by the energy consumption per task. 
    The result is a dictionary that assigns a list of tasks to each client.
    """
    # set global vars
    global task_list 
    global df_client_power
    global df_client_efficiency

    client_efficiency = calculate_average_efficiency()

    # aggregation: each client only once with aggregated energy efficiency values, we cannot use the power df because we want aggregated data (historically changing and not the latest!)
    df_client_energy = df_client_power.groupby("client_id", as_index=False).sum()
    
    # calc efficiency (power per task) -> the smaller the better
    df_client_energy["efficiency_per_tasks"] = df_client_energy["total_power_usage"] / df_client_energy["tasks_assigned"]

    # sort clients by efficiency (the lower values the better, this client is higher up)
    df_sorted = df_client_energy.sort_values(by="efficiency_per_task")

    # Extract efficiency data
    sorted_clients = df_sorted["client_id"].tolist()
    efficiencies = df_client_energy["efficiency_per_task"].to_numpy() # use to distribute tasks

    # Calculate inverse efficiency (as smaller values are better)
    # clients with higher efficiency are preferred, the inverse leads to larger values for more efficient clients
    # I have actually already calculated this in efficiencies
    inv_efficiencies = 1 / efficiencies
    probabilities = inv_efficiencies / inv_efficiencies.sum() # Normalisieren, Wahrscheinlichkeiten bestimmten wie die Aufgaben basierend auf der Effizienz an die Clients verteilt werden

    # distribute tasks randomly, but based on efficiency
    # shuffle the tasks, create thresholds and then split the shuffled task list accordingly
    # size of sublists based on probabilities, i.e. more efficient clients get more tasks, but every client gets tasks

    # SMake sure that the indices are integers, so we can split the array on the indices (they are not floats)
    # cummulative probabilities
    indices = np.cumsum(probabilities[:-1]) * len(task_list)
    indices = indices.astype(int)  # Umwandlung in Ganzzahlen

    sublists = np.array_split(np.random.permutation(task_list), indices)
    # sublists = np.array_split(np.random.permutation(task_list), np.cumsum(probabilities[:-1]) * len(task_list))

    # Save the sorting of the clients with their efficiencies in the df
    iteration = len(df_client_efficiency) + 1  # increase iteration for each new distrib
    for client_id, inv_efficiency in zip(sorted_clients, inv_efficiencies):
        # add new data to the df
        new_data = pd.DataFrame([{
            "iteration": iteration,
            "client_id": client_id,
            "efficiency_per_task": float(inv_efficiency),
        }])

        if not df_client_efficiency.empty and not df_client_efficiency.isna().all().all():
            df_client_efficiency = pd.concat([df_client_efficiency, new_data], ignore_index=True)
        else:
            df_client_efficiency = new_data.copy()  # on the first iter it will be empty and therefore would through a warning so set it directly!

    # combines the sorted clients with the corresponding task parts lists, each client receives a task list
    client_task_dict = {client: list(tasks) for client, tasks in zip(sorted_clients, sublists)}
    return client_task_dict

def distribute_tasks_to_clients(client, task_distribution):
    """Sends the distributed tasks to the respective clients"""
    global task_count

    for client_id, tasks in task_distribution.items():
        if client_id in connected_clients:
            task_string = "\n".join(tasks)
            topic = f"tasks/{client_id}"
            client.publish(topic, task_string, qos=1)
            client.publish("start_stop/taskWorker", 1, qos=1)
            start_task_session(client_id)
            task_count[client_id] = len(tasks)
            print(f"📤 Sent: {len(tasks)} tasks to {client_id} via topic: {topic}")
            client_status[client_id] = 1  # Set status to 1 (tasks started)
        else:
            print(f"⚠️ Client {client_id} is not connected. Tasks wont be sent.")

def run_task_distribution(client):
    """
    Decides which distribution to do. 
    On the first run we are going for a random distribution to collect some energy
    data and after that we will work with the collected data to distribute
    more intelligent.
    """

    global task_list
    global task_num

    while not stop_event.is_set():
        if not connected_clients:
            print("No connected clients. Waiting...")
            time.sleep(5)
            continue
    
        with task_lock:
            if not task_list:
                print("No tasks available. Waiting for new tasks...")
                time.sleep(5)
                continue
            if task_list: 
                task_num = len(task_list)

                # Calculation of the average efficiency
                efficiency = calculate_average_efficiency()

                if not efficiency:
                    # random distribution to the clients, we first have to write something in our dict
                    task_distribution = distribute_tasks_randomly()
                    # print all clients and their tasks:
                    # for client_name, tasks in task_distribution.items():
                    #     print(f"{client_name} gets: {tasks}")
                else:
                    # Clients nach Energieeffizienz sortieren (höchste zuerst)
                    task_distribution = distribute_tasks_by_efficiency()
                    # for client_name, tasks in task_distribution.items():
                    #     print(f"{client_name} gets: {tasks}")

                # send tasks to the clients
                distribute_tasks_to_clients(client, task_distribution)
                task_list.clear()

# send pings to clients
def send_ping(client):
    while not stop_event.is_set():
        ping_event.wait()  # Wait until event starts
        client.publish("ping/request", "Ping from TaskManager", qos=1)
        time.sleep(10)

# monitor active clients
def monitor_clients():
    global ping_event

    while not stop_event.is_set():
        print(f"Active clients: {connected_clients}")
        if connected_clients and not ping_event.is_set():
            print("Clients connected. Resuming ping...")
            ping_event.set()  # Activate ping
        elif not connected_clients and ping_event.is_set():
            print("No clients connected. Pausing ping...")
            ping_event.clear()  # Pause ping
        time.sleep(10)

# Callback function for MQTT connection
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    client.subscribe(MQTT_Publish_Topic, qos=0)  # Channel to deal with tasks
    client.subscribe(MQTT_Result_Topic, qos=0)
    client.subscribe("status/#")  # Subscribe to the status of all clients to monitor who is connected
    client.subscribe("ping/response/#")  # Listen for ping responses
    client.subscribe(MQTT_Task_Generator_Topic, qos=0)  # Listen to the task_generator
    # client.subscribe("devices/mac")
    client.subscribe("ShellyVerbrauch/#")  # Subscribe to all Shelly power topics
    client.subscribe("finish/#")

def get_shelly_apower_data(topic, message):
   # Parse the client ID from the topic
    client_id_json = topic.split("/")[1]

    if client_id_json in connected_clients:
        try:
            power_reading = json.loads(message)
            # check whether the message actually contains performance data
            if message == "true" or message == "false":
                print(f"ℹ️ Message received without performance data: {message}")
            elif "params" in power_reading:
                params = power_reading["params"]

                if "switch:0" in params:
                    actual_power = params["switch:0"].get("apower")

                    if actual_power is not None:
                        record_power_usage(client_id_json, actual_power)
                        print(f"🔹 {client_id_json}: {actual_power} W")
                    else:
                        print(f"⚠️ No 'apower'-data for {client_id_json}!")
                else:
                    print(f"ℹ️ 'params' available, but no 'switch:0': {message}")
            else:
                print(f"ℹ️ Message without 'params': {message}")      
        except json.JSONDecodeError:
            print(f"⚠️ Error parsing the JSON message: {message}")
        except Exception as e:
            print(f"⚠️ Unexpected error when processing {topic}: {e}")

# Callback when receiving messages
def on_message(client, userdata, msg):
    global task_list
    global task_count
    global finisher_counter
    global task_num

    message = msg.payload.decode()
    topic = msg.topic

    print(f"Message received on {msg.topic}: {message}")

    # count messages on the results topic
    # Check if the client finished the task
    if topic.startswith("finish/"):
        finisher_counter += 1
        if message.startswith("Finished"):
            finished_client = message.split(" ")[1]  # Assuming the message is something like "Finished ClientName"
            client_status[finished_client] = 0  # Set status to 0 (tasks completed)
            end_time = time.time()
            end_task_session(finished_client, end_time)
        if finisher_counter == len(connected_clients):
            print(f"All tasks have been processed: done_tasks = {finisher_counter}, init_tasks {task_num}")
            client.publish("start_stop/taskWorker", 0, qos=1) # status=0 when all clients worked the tasks

            # change the n when more clients are connected!!!!
            aggregate_last_n_entries(1)

            # Log directory for results
            project_root = os.getcwd()  # main directory

            # adapt directory to windows or linux depending on where it is running 
            log_directory_power = r"C:\Users\lenag\Documents\power-logs-green_thesis"

            if not os.path.exists(log_directory_power):
               os.makedirs(log_directory_power, exist_ok=True)

            # print data to the csv file for doku
            timestamp = time.strftime("%Y-%m-%d %H-%M-%S")

            file = f"{timestamp}_power-log-green_{task_num}.csv"
            file_path = os.path.join(log_directory_power, file)
            print("Printing Power Data so CSV in Path:" + file_path)

            with write_to_power_log_lock:
               file = df_client_power.to_csv(file_path, index=False, encoding="utf-8")
               print("created file")

    # Check for status messages
    if topic.startswith("status/"):
        client_name = topic.split("/")[1]
        if "Disconnected" in message:
            connected_clients.discard(client_name)
        elif "Connected" in message:
            connected_clients.add(client_name)
    # Check for ping answers
    elif topic.startswith("ping/response/"):
        client_name = topic.split("/")[-1]
        connected_clients.add(client_name) #TODO do i need that
    # Extract task_gen messages
    elif topic.startswith("task_generator"):
        print("Task generator triggered. Loading tasks...")
        load_tasks_from_file()
    # Handle power data from Shelly devices
    elif topic.startswith("ShellyVerbrauch/") and "events" in topic and "rpc" in topic:
        get_shelly_apower_data(topic, message)

    match = re.search(r"My name is (\w+)", message)
    if match:
        connected_pc = match.group(1)
        connected_clients.add(connected_pc)

# TODO: Load balancing logic based on power usage
# def make_load_decisions():
#     while not stop_event.is_set():
#         # Analyze power_usage_data to make decisions
#         for client_id, power_readings in power_tracking.items():
#             avg_power = sum(power_readings) / len(power_readings) if power_readings else 0
#             print(f"Average power for {client_id}: {avg_power} W")

#         # Sleep or wait for a specific condition to repeat the analysis
#         time.sleep(10)

if __name__ == '__main__':
    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(username=MQTT_Username, password=MQTT_Password)

    # Set Last Will Message so the manager knows where not to give tasks anymore
    client.will_set(f"status/{client_name}", "Disconnected", qos=1, retain=True)

    MQTT_Broker = get_broker_ip_via_file()
    Broker_Port = 1883

    try:
        # Connect to MQTT broker
        client.connect(MQTT_Broker, Broker_Port)

        # Start ping, monitoring, tasks, and load balancing threads
        ping_thread = threading.Thread(target=send_ping, args=(client,))
        monitor_thread = threading.Thread(target=monitor_clients)
        task_thread = threading.Thread(target=run_task_distribution, args=(client,))
        # load_balancing_thread = threading.Thread(target=make_load_decisions)

        ping_thread.start()
        monitor_thread.start()
        task_thread.start()
        # load_balancing_thread.start()

        client.loop_forever()

        # Publish initial messages
        client.publish(MQTT_Publish_Topic, f"This is the Manager. My name is {client_name} and I have subscribed to topic {MQTT_Publish_Topic}.")
        client.publish(MQTT_Result_Topic, f"This is the Manager. My name is {client_name} and I have subscribed to topic {MQTT_Result_Topic}.")

    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Exiting gracefully...")
    except Exception as e:
        print("Caught Exception " + e)
    finally:
        # adapt directory to windows or linux depending on where it is running 
        log_directory_efficiency = r"C:\Users\lenag\Documents\efficiency-logs-green_thesis"

        if not os.path.exists(log_directory_efficiency):
            os.makedirs(log_directory_efficiency, exist_ok=True)

        # print efficiency data to the csv file for doku
        timestamp = time.strftime("%Y-%m-%d %H-%M-%S")

        file = f"{timestamp}_clients_efficiencies.csv"
        file_path = os.path.join(log_directory_efficiency, file)
        print("Printing Power Data so CSV in Path:" + file_path)

        with write_to_power_log_lock:
            file = df_client_efficiency.to_csv(file_path, index=False, encoding="utf-8")
            print("created file")

        # end events and stop client
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
        # if 'load_balancing_thread' in locals() and load_balancing_thread.is_alive():
            # load_balancing_thread.join(timeout=5)

        print("Threads joined. Exiting now.")
        sys.exit(0)

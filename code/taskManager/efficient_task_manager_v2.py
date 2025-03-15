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
clients_with_tasks = 0

start_distribution = time.time()

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
# columns = [
#    "client_id", 
#    "total_power_usage", 
#    "avg_power",
#    "avg_power_per_second",
#    "avg_power_per_minute",
#    "kwh",
#    "relevant_power_values",
#    "num_of_power_values", # how many values were collected during the process
#    "tasks_assigned", 
#    "efficiency_per_task", # power in watt per task
#    "efficiency", # inverted eff
#    "total_duration", 
#    "time_per_task"]

# Create an empty DataFrame to store the consumption data
columns = [
   "client_id", 
   "total_power_usage", 
   "kwh",
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

# this is needed when the client got no tasks
client_ids = ["444626", "283436", "854514", "943099", "956975"] 
idle_power_values = [11.36, 2.9, 11.315, 4.2, 72.7]  # Idle Power Values -> you have to collect them beforehand

df_idle_power = pd.DataFrame({
    "client_id": client_ids,
    "idle_power_value": idle_power_values
})

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

def start_task_session(client_id):
   """Initializes a new measurement series for a client"""
   power_tracking[client_id] = []  # Empty list for measured current values
   task_count[client_id] = 0  # reset task number
   task_timing[client_id].append({"start_time": time.time()})  # save start time

def record_power_usage(client_id, power_value):
   """Saves individual power consumption values during processing"""
   if client_id in task_timing and task_timing[client_id]:  # start task
      power_tracking[client_id].append((time.time(), power_value))  # save time stamps

def get_historical_mean_power_all_clients():
    average_last_avg_power = df_client_power.groupby("client_id")["avg_power"].mean()
    return average_last_avg_power.to_dict()

def get_historical_mean_power_one_client(client_id):
    historical_power_values = get_historical_mean_power_all_clients()
    return historical_power_values.get(client_id, None) # the avg of every avg_power_value for this client

def end_task_session(client_id, end_time):
    """Called when a client reports that it is ready"""
    global df_client_power

    avg_power = 0

    if client_id not in power_tracking:
       print(f"⚠️ No Power-Tracking for {client_id} found!")
       return

    start_time = task_timing[client_id][-1]["start_time"]  # get start time

    # Only add up values within the time window
    relevant_power_values = [
       power for timestamp, power in power_tracking[client_id] 
       if start_time <= timestamp <= end_time
    ]
    
    if len(relevant_power_values) == 0:
        # no values from shelly there
        avg_power = get_historical_mean_power_one_client(client_id)
        if not avg_power:
            idle_power_value_list = df_idle_power.loc[df_idle_power['client_id'] == client_id, 'idle_power_value'].values
        
            # Check if there's a valid idle_power_value
            if len(idle_power_value_list) > 0 and idle_power_value_list[0] is not None:
                avg_power = float(idle_power_value_list[0])
            else:
                print(f"⚠️ No valid idle power value for client {client_id}. Using default value.")
                avg_power = 0.0  # Set to a default value if None
    else:
      avg_power = np.mean(relevant_power_values)  # Durchschnittliche Leistung des Clients, mean wird genommen, weil manchmal einer und manchmal 30 Datenpunkte kommen
      
      if avg_power is not None:
        avg_power = float(avg_power)

    # is in seconds because time is in epoch, this Unix timestamp
    total_duration = end_time - start_time

    # Power (watts) × time (seconds) → watt seconds (Ws)
    # 1 kWh = 1,000 watts × 1 hour = 3,600,000 watt seconds (Ws)
    # Therefore, we divide by 3,600,000 to get from Ws → kWh.
    total_power_usage = avg_power * total_duration  # Total energy consumption in Ws
    kwh = (avg_power * total_duration) / 3600000  # Conversion to kWh 

    total_tasks = task_count.get(client_id, 0)
    
    # tasks per kWh
    inv_efficiency = total_tasks / kwh if kwh > 0 else 0 # tasks per kWh (inv_efficiency) is betetr, wenn ich wissen will, wer am meisten aus der Energie rausholt.
    #  I would rather use inv_efficiency (tasks per kWh) because it shows more clearly who has the highest productivity with the lowest energy consumption.

    # kwh per task
    efficiency_per_task = kwh / total_tasks if total_tasks > 0 else 0  # kWh per task (efficiency_per_task) ist gut, wenn ich wissen will, wer die geringste Energiemenge pro Task benötigt
    time_per_task = total_duration / total_tasks if total_tasks > 0 else 0 
    
    # add new data to dataframe
    new_data = pd.DataFrame([{
       "client_id": client_id,
       "total_power_usage": total_power_usage,
       "avg_power": avg_power, # avg_power in this run
       "kwh": kwh, # in this run
       "relevant_power_values": relevant_power_values,
       "num_of_power_values": len(relevant_power_values), 
       "tasks_assigned": total_tasks,
       "efficiency_per_task": efficiency_per_task, # kwh per task
       "efficiency": inv_efficiency, # tasks per kWh
       "total_duration": total_duration,
       "time_per_task": time_per_task
    }])

    df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)

    print(f"✅ Save Data for {client_id}: {new_data.to_dict(orient='records')}")

    # Empty memory for the next measurement
    del power_tracking[client_id]

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
    last_n_entries = df_client_power.tail(n)
    
    if last_n_entries.empty:
       # print(f"Found no last {n} entries for client {client_id}.")
       print("No last entries found.")
       return None  # return empty dataframe row
    
    # calculate sums and means of the values
    total_power = last_n_entries["total_power_usage"].sum()
    avg_power = total_power / n
    total_tasks = last_n_entries["tasks_assigned"].sum()
    total_power_values = last_n_entries["num_of_power_values"].sum()
    total_kwh = last_n_entries["kwh"].sum() # kwh summiert für das gesamte Netzwerk -> danach dann verteilen, immer an den mehr aufgaben, der am ende weniger kwh verbraucht hat
    
    avg_efficiency_per_task = last_n_entries["efficiency_per_task"].mean()
    avg_inv_efficiency = last_n_entries["efficiency"].mean()
    avg_time_per_task = last_n_entries["time_per_task"].mean()
    avg_duration = last_n_entries["total_duration"].mean()
    # avg_power_values = last_n_entries["num_of_power_values"].mean()
    
    # create new df row with aggregated data
    new_data = pd.DataFrame([{
       "client_id": 0,
       "total_power_usage": total_power,
       "avg_power": avg_power, # avg_power of the network
       "kwh": total_kwh, # power for the whole network
       "relevant_power_values": None,
       "num_of_power_values": total_power_values, 
       "tasks_assigned": total_tasks,
       "efficiency_per_task": avg_efficiency_per_task,
       "efficiency": avg_inv_efficiency,
       "total_duration": avg_duration, # we use avg_duration, this is much more logical
       "time_per_task": avg_time_per_task
    }])
    
    df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)

# read tasks from file and populate task_listt
def load_tasks_from_file():
    global task_list
    global task_count
    global timestamp_file
    global finisher_counter
    global clients_with_tasks
    global task_num

    finisher_counter = 0
    clients_with_tasks = 0

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
                task_num = len(loaded_tasks)

                # Extend every task with sender and receiver
                task_list = [
                    f"{task} sender={client_name}, receiver=X\""
                    for task in loaded_tasks
                ]
        print(f"Loaded tasks.")
        task_num = len(loaded_tasks)
    except Exception as e:
        print(f"File {task_file} not found Error loading tasks:{e}.")

def distribute_tasks_randomly():
    global task_list 
    global connected_clients
    global clients_with_tasks
    global task_count

    # random.shuffle(task_list)  # Aufgaben zufällig mischen
    split_indices = sorted(random.sample(range(1, len(task_list)), len(connected_clients) - 1))  # Zufällige Trennstellen setzen
    sublists = [task_list[i:j] for i, j in zip([0] + split_indices, split_indices + [None])]
    
    client_task_dict = {client: tasks for client, tasks in zip(connected_clients, sublists)}
    # task_count = {client: len(tasks) for client, tasks in client_task_dict.items()}
    # number of clients with at least one task
    # everytime if tasks true add 1 to the sum 
    clients_with_tasks = sum(1 for tasks in client_task_dict.values() if tasks)
    return client_task_dict

# TODO kalkulieren von kwh average und danach verteilen!!!
def calculate_average_efficiency():
    global df_client_power

    if df_client_power.empty:
        return {}

    # calculation of the average per client
    avg_efficiency = df_client_power.groupby("client_id").agg(
        total_tasks=("tasks_assigned", "sum"),
        total_power=("total_power_usage", "sum")
    )

    # efficiency = tasks per kWh
    avg_efficiency["efficiency"] = avg_efficiency["total_tasks"] / avg_efficiency["total_power"]
    
    # convert to dictionary that can then be used for intelligent task distribution
    return avg_efficiency["efficiency"].to_dict()

def calculate_historical_efficiency():
    """
    Calculates the historical inverse efficiency of each client (lower values are more efficient).
    Returns: A dictionary with the `client_id` and their `inv_efficiency` (inverse efficiency values).
    """

    global df_client_power

    df_client_power_filtered = df_client_power[df_client_power["client_id"] != 0]
    avg_inv_eff = df_client_power_filtered.groupby("client_id")["efficiency_per_task"].mean() # efficiency = tasks per kWh
    return avg_inv_eff.to_dict()

def calc_historcial_avg_kwh():
    global df_client_power  # DataFrame mit Power-Daten

    # Berechne den durchschnittlichen kWh-Verbrauch pro Client (groupby auf client_id)
    df_client_power["avg_kwh_per_client"] = df_client_power.groupby("client_id")["kwh"].transform("mean")

# TODO kalkulieren von kwh average und danach verteilen!!! mh oder wir nehmen halt die effizienz 
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

    client_inv_efficiency = calculate_historical_efficiency()

    # Sort clients according to their inverse efficienc.. more efficient clients have lower values, but higher “inverse” values
    sorted_clients = sorted(client_inv_efficiency, key=client_inv_efficiency.get, reverse=True)
    
    # get inverse efficiency (as smaller values are better)
    # clients with higher efficiency are preferred, the inverse leads to larger values for more efficient clients
    inv_efficiencies = np.array([client_inv_efficiency[client] for client in sorted_clients])
    
    # Normalize, probabilities determine how tasks are distributed to clients based on efficiency
    probabilities = inv_efficiencies / inv_efficiencies.sum() 

    num_clients = len(sorted_clients)
    num_tasks = len(task_list)

    client_task_dict = {client: [] for client in sorted_clients}

    # First assign a task to each client if there are enough tasks
    if num_tasks >= num_clients:
        # Distribute the first 'num_clients' tasks
        for i in range(num_clients):
            client_task_dict[sorted_clients[i]].append(task_list[i])
        
        # Remove the distributed tasks from the task_list
        remaining_tasks = task_list[num_clients:]
    else:
        # Wenn weniger Aufgaben als Clients vorhanden sind, verteile die vorhandenen Aufgaben
        remaining_tasks = task_list

    task_counts = (probabilities * len(remaining_tasks)).astype(int)
    task_counts[-1] = len(remaining_tasks) - task_counts.sum()

    # Create a list of tasks for each client based on the probabilities
    start_index = 0
    for i, task_count in enumerate(task_counts):
        client_task_dict[sorted_clients[i]].extend(remaining_tasks[start_index:start_index + task_count])
        start_index += task_count

    # Save the sorting of the clients with their efficiencies in the df
    iteration = len(df_client_efficiency) + 1  # increase iteration for each new distrib
    
    for client_id in sorted_clients:
        # get the historical inv_eff
        inv_efficiency = client_inv_efficiency[client_id]
        
        # add new data
        new_data = pd.DataFrame([{
            "iteration": iteration,
            "client_id": client_id,
            "inv_efficiency": float(inv_efficiency),  # Speichern der inversen Effizienz
        }])

        if not df_client_efficiency.empty and not df_client_efficiency.isna().all().all():
            df_client_efficiency = pd.concat([df_client_efficiency, new_data], ignore_index=True)
        else:
            df_client_efficiency = new_data.copy()  # on the first iter it will be empty and therefore would through a warning so set it directly!

    # combines the sorted clients with the corresponding task parts lists, each client receives a task list
    # client_task_dict = {client: list(tasks) for client, tasks in zip(sorted_clients, sublists)}
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
    global start_distribution

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
                    start_distribution = time.time()
                    task_distribution = distribute_tasks_randomly()
                    # print all clients and their tasks:
                    # for client_name, tasks in task_distribution.items():
                    #     print(f"{client_name} gets: {tasks}")
                else:
                    # Clients nach Energieeffizienz sortieren (höchste zuerst)               
                    start_distribution = time.time()
                    task_distribution = distribute_tasks_by_efficiency()
                    # for client_name, tasks in task_distribution.items():
                    #     print(f"{client_name} gets: {tasks}")

                # send tasks to the clients
                distribute_tasks_to_clients(client, task_distribution)
                task_list.clear()

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
    last_n_entries = df_client_power.tail(n)

    if last_n_entries.empty:
       # print(f"Found no last {n} entries for client {client_id}.")
       print("No last entries found.")
       return None  # return empty dataframe row

    # calculate sums and means of the values
    total_power = last_n_entries["total_power_usage"].sum()
    avg_power = total_power / n
    total_tasks = last_n_entries["tasks_assigned"].sum()
    total_power_values = last_n_entries["num_of_power_values"].sum()
    total_kwh = last_n_entries["kwh"].sum() # kwh summiert für das gesamte Netzwerk -> danach dann verteilen, immer an den mehr aufgaben, der am ende weniger kwh verbraucht hat
    
    avg_efficiency_per_task = last_n_entries["efficiency_per_task"].mean()
    avg_inv_efficiency = last_n_entries["efficiency"].mean()
    avg_time_per_task = last_n_entries["time_per_task"].mean()
    avg_duration = last_n_entries["total_duration"].mean()
    # avg_power_values = last_n_entries["num_of_power_values"].mean()

    # create new df row with aggregated data
    new_data = pd.DataFrame([{
        "client_id": 0,
        "total_power_usage": total_power,
        "avg_power": avg_power, # avg_power of the network
        "kwh": total_kwh, # power for the whole network
        "relevant_power_values": None,
        "num_of_power_values": total_power_values, 
        "tasks_assigned": total_tasks,
        "efficiency_per_task": avg_efficiency_per_task,
        "efficiency": avg_inv_efficiency,
        "total_duration": avg_duration, # we use avg_duration, this is much more logical
        "time_per_task": avg_time_per_task
    }])

    df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)

# monitor active clients
def monitor_clients():
    # global ping_event
    while not stop_event.is_set():
        print(f"Active clients: {connected_clients}")
        # if connected_clients and not ping_event.is_set():
        #    print("Clients connected. Resuming ping...")
        #    ping_event.set()  # activate ping
        # elif not connected_clients and ping_event.is_set():
        #    print("No clients connected. Pausing ping...")
        #    ping_event.clear()  # pause ping
        time.sleep(10)

# callback function for MQTT connection
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    connected_clients.clear()  # empty set when we are setting a new connection

    client.subscribe(MQTT_Publish_Topic, qos=0)  # Channel to deal with tasks
    client.subscribe(MQTT_Result_Topic, qos=0)
    client.subscribe("status/#")  # Subscribe to the status of all clients to monitor who is connected
    # client.subscribe("ping/response/#")  # Listen for ping responses
    client.subscribe("task_generator", qos=1) # listen to the task_generator    # client.subscribe("devices/mac")
    client.subscribe("ShellyVerbrauch/#")  # Subscribe to all Shelly power topics
    client.subscribe("finish/#")

def get_shelly_apower_data_status_switch(topic, message):
    # Parse the client ID from the topic
    client_id_json = topic.split("/")[1]

    if client_id_json in connected_clients:
       try:
            power_reading = json.loads(message)
            # Check whether the message actually contains performance data
            if message == "true" or message == "false":
               print(f"ℹ️ Message received without performance data: {message}")
            elif "apower" in power_reading:
                actual_power = power_reading["apower"]

                if actual_power is not None:
                   record_power_usage(client_id_json, actual_power)
                   print(f"🔹 {client_id_json}: {actual_power} W")
                else:
                   print(f"⚠️ No 'apower' data for {client_id_json}!")
                   # else:
                   #    print(f"ℹ️ 'params' available, but no 'switch:0': {message}")
            else:
                print(f"ℹ️ Messagge without 'params': {message}")     
       except json.JSONDecodeError:
            print(f"⚠️ Error parsing the JSON message: {message}")
       except Exception as e:
            print(f"⚠️ Unexpected error when processing {topic}: {e}")

def get_shelly_apower_data_events(topic, message):
    # Parse the client ID from the topic
    client_id_json = topic.split("/")[1]

    if client_id_json in connected_clients:
        try:
            power_reading = json.loads(message)
           # Check whether the message actually contains performance data
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
                        print(f"⚠️ No 'apower' data for {client_id_json}!")
                else:
                    print(f"ℹ️ 'params' available, but no 'switch:0': {message}")
            else:
                print(f"ℹ️ Messagge without 'params': {message}")     
        except json.JSONDecodeError:
                print(f"⚠️ Error parsing the JSON message: {message}")
        except Exception as e:
              print(f"⚠️ Unexpected error when processing {topic}: {e}")

def handle_idle_clients(duration):
    """Handle clients that have no tasks assigned and fill idle values."""
    global df_client_power
    global df_idle_power

    for client_id in client_ids:  # iterate through the client_ids
        # check if the client is not listed in the task_count or has no tasks assigned
        if task_count.get(client_id, 0) == 0 or client_id not in task_count:
            # get the idle power value from the df_idle_power DataFrame
            idle_power_value_list = df_idle_power.loc[df_idle_power['client_id'] == client_id, 'idle_power_value'].values
      
            # Check if there's a valid idle_power_value
            if len(idle_power_value_list) > 0 and idle_power_value_list[0] is not None:
               idle_power_value = float(idle_power_value_list[0])
            else:
               print(f"⚠️ No valid idle power value for client {client_id}. Using default value.")
               idle_power_value = 0.0  # Set to a default value if None

            # Create a new row for this client with idle power values
            new_data = pd.DataFrame([{
               "client_id": client_id,
               "total_power_usage": idle_power_value * duration,
               "avg_power": idle_power_value,  # Idle power is considered as average power
               "kwh": (idle_power_value * duration) / 3600000,  # Example calculation to get kWh
               "relevant_power_values": [idle_power_value],
               "num_of_power_values": 1,  # Only one value (idle power)
               "tasks_assigned": 0,  # No tasks assigned
               "efficiency_per_task": 0,  # Efficiency would be 0 as no tasks were assigned
               "efficiency": 0,  # Inverted efficiency would also be 0
               "total_duration": duration,  # Placeholder for total duration (e.g., 1 hour for idle time)
               "time_per_task": 0  # No tasks, so no time per task
            }])

            # Append this data to the DataFrame
            df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)

            print(f"✅ Added Idle Data for {client_id}: {new_data.to_dict(orient='records')}")


# Callback when receiving messages
def on_message(client, userdata, msg):
    global task_list
    global task_count
    global finisher_counter
    global task_num
    global clients_with_tasks
    global stop_distribution

    message = msg.payload.decode()
    topic = msg.topic

    if not topic.startswith("ShellyVerbrauch"):
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
        if clients_with_tasks == finisher_counter:
            stop_distribution = time.time()

            print(f"All tasks have been processed: done_tasks = {finisher_counter}, init_tasks {task_num}")
            client.publish("start_stop/taskWorker", 0, qos=1) # status=0 when all clients worked the tasks
            client.publish("tasks_done", "done", qos=1) # publish message to tg to trigger new task batch

            print("handling idle clients..")
            duration = stop_distribution - start_distribution
            handle_idle_clients(duration)

            # change the n when more clients are connected!!!!
            aggregate_last_n_entries(len(connected_clients))
            task_count.clear() # set this to clear hear and not in end_task_session cause we need the values to handle idle clients

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

    # check for status messages
    if topic.startswith("status/"):
        client_name = topic.split("/")[1]
        if "Disconnected" in message:
           connected_clients.discard(client_name)
        elif "Connected" in message:
           connected_clients.add(client_name)
    # extract task_gen messages
    elif topic == "task_generator":
        print("Task generator triggered. Loading tasks...")
        load_tasks_from_file()
    # Handle power data from Shelly devices
    elif topic.startswith("ShellyVerbrauch/") and "status" in topic and "switch:0" in topic:
        get_shelly_apower_data_status_switch(topic, message)
    elif topic.startswith("ShellyVerbrauch/") and "events" in topic:
        get_shelly_apower_data_events(topic, message)
    
    match = re.search(r"My name is (\w+)", message)
    if match:
        connected_pc = match.group(1)
        connected_clients.add(connected_pc)

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

        # Start monitoring, tasks, and load balancing threads
        monitor_thread = threading.Thread(target=monitor_clients)
        task_thread = threading.Thread(target=run_task_distribution, args=(client,))
        monitor_thread.start()
        task_thread.start()

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
        stop_event.set()
        task_event.set()
        print("Set ping_event and stop_event to False.")
        client.loop_stop()
        print("Stopped client loop.")
        client.disconnect()
        print("Client disconnected.")

        # Join threads only if they are alive
        if 'monitor_thread' in locals() and monitor_thread.is_alive():
            monitor_thread.join(timeout=5)
        if 'task_thread' in locals() and task_thread.is_alive():
            task_thread.join(timeout=5)
        # if 'load_balancing_thread' in locals() and load_balancing_thread.is_alive():
            # load_balancing_thread.join(timeout=5)

        print("Threads joined. Exiting now.")
        sys.exit(0)

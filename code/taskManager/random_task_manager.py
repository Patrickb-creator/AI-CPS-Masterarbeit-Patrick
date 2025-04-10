"""
Random strategy to distribute AI tasks in a MQTT network between different clients.
This script is designed to work with the MQTT protocol and is intended to be run on a machine that acts as a task manager in a distributed system.
It connects to an MQTT broker, subscribes to relevant topics, and distributes tasks to connected clients based on their availability.
It also monitors the power consumption of the clients using Shelly devices and logs the results for analysis.
"""
import paho.mqtt.client as mqtt
import os
import re
import sys
import time
import threading
import random
import pandas as pd
import numpy as np
import json
from collections import defaultdict

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
connected_clients = set()  # unique set of connected PCs
clients_with_tasks = 0

start_distribution = time.time()

# ping_event = threading.Event()  # event to control ping threads
stop_event = threading.Event()
task_event = threading.Event()

finisher_counter = 0  # Counter for received messages on results topic
task_num = 0  # Counter for loaded tasks

timestamp_file = None
log_lock = threading.Lock() # Ensure logging 
write_to_power_log_lock = threading.Lock()

# Global task list
task_list = []
task_lock = threading.Lock()  # Ensure thread-safe access to task_list

client_status = defaultdict(int)  # 1: task distributed, 0: tasks completed

# Cache for current measured values per client
# This is only for the analysis in experiments!!!
power_tracking = defaultdict(list)  # Stores all measured power values per client
task_count = defaultdict(int)   # Stores how many tasks a client has received

# Cache for start and end time per Client and Batch
# This is only for the analysis in experiments!!!
task_timing = defaultdict(list)  

# Create an empty DataFrame to store the consumption data
# This is only for the analysis in experiments!!!
columns = [
   "client_id", 
   "total_power_usage", 
   "kwh",
   "relevant_power_values",
   "num_of_power_values", # how many values were collected during the process
   "tasks_assigned", 
   "efficiency_per_task", # power in watt per task
   "efficiency", # efficiency in tasks per kWh
   "total_duration", 
   "time_per_task"]

df_client_power = pd.DataFrame(columns=columns)

# This is needed when the client got no tasks
client_ids = ["444626", "283436", "854514", "943099", "956975"] 
idle_power_values = [11.36, 2.9, 11.315, 4.2, 72.7]  # Idle Power Values -> you have to collect them beforehand

df_idle_power = pd.DataFrame({
    "client_id": client_ids,
    "idle_power_value": idle_power_values
})

# Get the latest broker ip of the broker which was started via file -> Fallback Option
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
   """
   Initializes a new measurement series for a client
   """
   power_tracking[client_id] = []  # Empty list for measured current values
   task_count[client_id] = 0  # Reset task number
   task_timing[client_id].append({"start_time": time.time()})  # Save start time

def record_power_usage(client_id, power_value):
   """
   Saves individual power consumption values during processing
   """
   if client_id in task_timing and task_timing[client_id]:  # Start task
      power_tracking[client_id].append((time.time(), power_value))  # Save time stamps

def get_historical_mean_power_all_clients():
   average_last_avg_power = df_client_power.groupby("client_id")["avg_power"].mean()
   return average_last_avg_power.to_dict()

def get_historical_mean_power_one_client(client_id):
   historical_power_values = get_historical_mean_power_all_clients()
   return historical_power_values.get(client_id, None) # the avg of every avg_power_value for this client

def end_task_session(client_id, end_time):
   """
   Finalizes the task session for a given client, calculates energy consumption, 
   and stores the resulting data.

   This function is called when a client reports that it is ready. It calculates
   the average power usage within the session's time window, determines energy 
   consumption in kWh, and tracks efficiency metrics. 
   The function also stores the resulting data in a global dataframe and clears the power tracking.

   Args:
      client_id (str): The unique identifier for the client whose task session is being finalized.
      end_time (int): The Unix timestamp marking the end of the task session.

   Returns:
      None

   Side Effect:
      - Updates the global `df_client_power` dataframe with the calculated results.
   """
   global df_client_power
   global df_idle_power

   avg_power = 0

   if client_id not in power_tracking:
      print(f"No Power-Tracking for {client_id} found!")
      return

   start_time = task_timing[client_id][-1]["start_time"]  # get start time

   # Only add up values within the time window
   relevant_power_values = [
      power for timestamp, power in power_tracking[client_id] 
      if start_time <= timestamp <= end_time
   ]

   if len(relevant_power_values) == 0:
      # No values from shelly here
      avg_power = get_historical_mean_power_one_client(client_id)
      if not avg_power:
         idle_power_value_list = df_idle_power.loc[df_idle_power['client_id'] == client_id, 'idle_power_value'].values
        
         # Check if there's a valid idle_power_value
         if len(idle_power_value_list) > 0 and idle_power_value_list[0] is not None:
            avg_power = float(idle_power_value_list[0])
         else:
            print(f"No valid idle power value for client {client_id}. Using default value.")
            avg_power = 0.0  # Set to a default value if None
         # we take this when no power is provided so we at least get something..
   else:
      avg_power = np.mean(relevant_power_values)  # Average performance of the client, mean is taken because sometimes one and sometimes 30 data points are received
      
      if avg_power is not None:
         avg_power = float(avg_power)
   
   # Is in seconds because time is in epoch, this Unix timestamp
   total_duration = end_time - start_time

   # Power (watts) × time (seconds)-> watt seconds (Ws)
   # 1 kWh = 1,000 watts × 1 hour = 3,600,000 watt seconds (Ws)
   # Therefore, we divide by 3,600,000 to get from Ws -> kWh.
   total_power_usage = avg_power * total_duration  # Total energy consumption in Ws
   kwh = (avg_power * total_duration) / 3600000  # Conversion to kWh   

   total_tasks = task_count.get(client_id, 0)
   
   # Tasks per kWh
   inv_efficiency = total_tasks / kwh if kwh > 0 else 0 # # Tasks per kWh (inv_efficiency) is better, if you want to know who gets the most out of the energy.

   # kwh per task
   efficiency_per_task = kwh / total_tasks if total_tasks > 0 else 0  # kWh per task (efficiency_per_task) is good if I want to know who needs the least amount of energy per task
   time_per_task = total_duration / total_tasks if total_tasks > 0 else 0 
   
   # Add new data to dataframe
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

   # Create new df row with aggregated data
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
      "total_duration": avg_duration,
      "time_per_task": avg_time_per_task
   }])

   df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)

def load_tasks_from_file():
   """
   Loads tasks from a pre-generated file, assigns a sender and a random receiver to each task,
   and populates the global task list with these tasks.

   The function reads tasks from the file `generated_tasks.txt` located in the `taskGenerator` directory.
   Each task is then extended with a sender and a randomly chosen receiver (from the list of 
   connected clients). The tasks are stored in the global `task_list` variable.

   Global Variables:
      task_list (list): A list to store the tasks, each with a sender and receiver.
      task_count (dict): A dictionary to keep track of the task count (not modified in this function).
      timestamp_file (str): The file path for logging timestamps (not used in this function).
      finisher_counter (int): Counter for completed tasks, initialized to 0.
      clients_with_tasks (int): Counter for clients with tasks, initialized to 0.
      task_num (int): The total number of tasks loaded from the file.

   Exceptions:
      If the file `generated_tasks.txt` is not found or cannot be read, an error message is printed.
   """
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

   # Log dir for logs of the TM in regards of processing and distributing the tasks
   log_directory = os.path.join(manager_dir, 'logs')
   if not os.path.exists(log_directory):
      os.makedirs(log_directory)

   # Change this if you use different generators!!
   task_file = os.path.join(generator_dir, "generated_tasks.txt")

   try:
      with open(task_file, 'r', encoding='utf-8') as file:
         with task_lock:
            # Load tasks from file that was generated by task_generator
            loaded_tasks = [line.strip() for line in file.readlines() if line.strip()]
            task_num = len(loaded_tasks)

            # Extend every task with sender and random receiver -> only for random distribution
            task_list = [
               f"{task} sender={client_id}, receiver={random.choice(list(connected_clients))}\""
               for task in loaded_tasks
            ]
      print("Loaded tasks.")
   except Exception as e:
      print(f"File {task_file} not found Error loading tasks:{e}.")

def find_receiver(task):
   # Regular expression to extract the receiver value
   match = re.search(r'receiver=([^\s,]+)', task)
   # If a hit is found, output the receiver
   if match:
      receiver = match.group(1).rstrip('"')
      return receiver
   else:
      print("No receiver found")

# distrbute available tasks randomly to the connected clients
# the ❤️ of the distribution!!!!!
def distribute_tasks(client):
   """
   Distributes tasks from the global task list to connected clients.

   This function continuously checks the availability of connected clients and tasks in the task list. 
   It assigns tasks to clients based on the receiver specified in the task and sends the tasks to the appropriate 
   connected client. If a client is not connected, the task is skipped. The tasks for each client are grouped together 
   before being sent to the target client. The function also tracks the number of tasks assigned to each client.

   The task distribution is performed in a loop, which continues until a stop event is triggered. A small delay 
   is included between task distributions to avoid overwhelming the MQTT system.

   Args:
      client: The MQTT client instance used to publish task information to clients.

   Global Variables:
      task_list (list): A list of tasks available for distribution.
      clients_with_tasks (int): Counter tracking the number of clients that have received tasks.
      task_count (dict): A dictionary storing the count of tasks assigned to each client.
      start_distribution (float): Timestamp marking the start of task distribution.

   Behavior:
      - The function waits for new tasks if the task list is empty or no clients are connected.
      - Tasks are grouped based on the receiver and sent to the connected client.
      - If a client is not connected, the task is skipped, and the function continues to the next task.
      - Each client is assigned a task session for measurement tracking once tasks are sent.

   Exceptions:
      - No specific exceptions are raised, but tasks are skipped if no clients are connected or if a task cannot be sent to the target client.
   """
   global task_list
   global clients_with_tasks
   global task_count
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
         
         while task_list:
            task = task_list.pop(0)  # Get the first task and release it from the task stack
            target_client = find_receiver(task)  # Find the receiver for this task

            # Create a list to hold all tasks for the same receiver
            combined_tasks = [task]  # Start with the first task
            
            # Find all other tasks with the same receiver and add them together
            i = 0
            next_target_client = ""

            while i < len(task_list):
               next_task = task_list[i]
               next_target_client = find_receiver(next_task)

               # If the receiver matches, add the task to the combined list
               if next_target_client == target_client:
                  combined_tasks.append(next_task)
                  # Remove the task from the list, so it doesn´t appear twice
                  task_list.pop(i)
               else:
                  # Otherwise, move to the next task in the list
                  i += 1

            number_of_tasks = len(combined_tasks)

            # Combine all tasks for the receiver into one string to send them together
            task_string = "\n".join(combined_tasks)

            # check if the target client is connected and then send task to the right client
            if target_client in connected_clients:
               client.publish("start_stop/taskWorker", 1, qos=1) # Status=1 cause it starts to distribute tasks
               client.publish(f"tasks/{target_client}", task_string, qos=1)
               start_task_session(target_client) #  Init a new measurement series for the client, only for analysis
               print(f"Sent {number_of_tasks} tasks to {target_client}.")
               task_count[target_client] = number_of_tasks # set task counter
               clients_with_tasks += 1 # one client more who got tasks
               start_distribution = time.time() # starting counting the time for idle clients, only for analysis
            else:
               print(f"Target client {target_client} is not connected. Skipping.")
            time.sleep(2) # Small delay to avoid overwhelming mqtt

# Monitor active clients
def monitor_clients():
   # global ping_event, if you want to use it, comment in the ping stuff
   while not stop_event.is_set():
      print(f"Active clients: {connected_clients}")
      # if connected_clients and not ping_event.is_set():
      #    print("Clients connected. Resuming ping...")
      #    ping_event.set()  # activate ping
      # elif not connected_clients and ping_event.is_set():
      #    print("No clients connected. Pausing ping...")
      #    ping_event.clear()  # pause ping
      time.sleep(10)

# Callback function for mqtt connection
def on_connect(client, userdata, flags, rc):
   print("Connected with result code " + str(rc))

   connected_clients.clear()  # Empty set when we are setting a new connection

   client.subscribe(MQTT_Publish_Topic, qos=0) # Channel to deal with tasks
   client.subscribe(MQTT_Result_Topic, qos=0)
   client.subscribe("status/#") # Subscribe to the status of all clients to monitor who is connected
   # client.subscribe("ping/response/#") # Listen for ping responses
   client.subscribe("task_generator", qos=1) # Listen to the task_generator
   client.subscribe("ShellyVerbrauch/#")  # Subscribe to all Shelly power topics
   client.subscribe("finish/#")

def get_shelly_apower_data_status_switch(topic, message):
   """
   ONLY FOR ANALYSIS!!!
   Processes the power data received from the Shelly device when the status switch is triggered.
   Change this according to your monitoring device!
   
   Parses the incoming message for power usage data (`apower`) and records the data for the client.
   Handles error cases for missing or malformed data.

   Args:
      topic (str): The topic from which the message was received.
      message (str): The message containing the power usage data.

   Returns:
      None
   """
   # Parse the client ID from the topic
   client_id_json = topic.split("/")[1]

   if client_id_json in connected_clients:
      try:
         power_reading = json.loads(message)
         # Check whether the message actually contains performance data
         if message == "true" or message == "false":
            print(f"Message received without performance data: {message}")
         elif "apower" in power_reading:
            actual_power = power_reading["apower"]
   
            if actual_power is not None:
               record_power_usage(client_id_json, actual_power)
               print(f"🔹 {client_id_json}: {actual_power} W")
            else:
               print(f"No 'apower' data for {client_id_json}!")
         else:
               print(f"Messagge without 'params': {message}")     
      except json.JSONDecodeError:
            print(f"Error parsing the JSON message: {message}")
      except Exception as e:
            print(f"Unexpected error when processing {topic}: {e}")

def get_shelly_apower_data_events(topic, message):
   """
   ONLY FOR ANALYSIS!!!
   Processes the power data received from the Shelly device.
   Change this according to your monitoring device!
   
   Extracts the `apower` value from the message, records it, and prints relevant information.
   Handles error cases for missing or malformed data.

   Args:
      topic (str): The topic from which the message was received.
      message (str): The message containing the power usage data.

   Returns:
      None
   """
   # Parse the client ID from the topic
   client_id_json = topic.split("/")[1]

   if client_id_json in connected_clients:
      try:
         power_reading = json.loads(message)
         # Check whether the message actually contains performance data
         if message == "true" or message == "false":
            print(f"Message received without performance data: {message}")
         elif "params" in power_reading:
               params = power_reading["params"]
               
               if "switch:0" in params:
                  actual_power = params["switch:0"].get("apower")
      
                  if actual_power is not None:
                     record_power_usage(client_id_json, actual_power)
                     print(f"🔹 {client_id_json}: {actual_power} W")
                  else:
                     print(f"No 'apower' data for {client_id_json}!")
               else:
                  print(f"'params' available, but no 'switch:0': {message}")
         else:
            print(f"Messagge without 'params': {message}")     
      except json.JSONDecodeError:
            print(f"Error parsing the JSON message: {message}")
      except Exception as e:
            print(f"Unexpected error when processing {topic}: {e}")

def handle_idle_clients(duration):
   """
   ONLY FOR ANALYSIS!!!
   Handle clients that have no tasks assigned and fill idle values.
   """
   global df_client_power
   global df_idle_power

   for client_id in client_ids:  # Iterate through the client_ids
      # Check if the client is not listed in the task_count or has no tasks assigned
      if task_count.get(client_id, 0) == 0 or client_id not in task_count:
         # Get the idle power value from the df_idle_power DataFrame
         idle_power_value_list = df_idle_power.loc[df_idle_power['client_id'] == client_id, 'idle_power_value'].values
      
         # Check if there's a valid idle_power_value
         if len(idle_power_value_list) > 0 and idle_power_value_list[0] is not None:
            idle_power_value = float(idle_power_value_list[0])
         else:
            print(f"No valid idle power value for client {client_id}. Using default value.")
            idle_power_value = 0.0  # Set to a default value if None

         # Create a new row for this client with idle power values
         new_data = pd.DataFrame([{
            "client_id": client_id,
            "total_power_usage": idle_power_value * duration,
            "avg_power": idle_power_value,  # Idle power is considered as average power
            "kwh": (idle_power_value * duration) / 3600000,  # calculation to get kWh
            "relevant_power_values": [idle_power_value],
            "num_of_power_values": 1,  # Only one value (idle power)
            "tasks_assigned": 0,  # No tasks assigned
            "efficiency_per_task": 0,  ## Efficiency per task would be 0 as no tasks were assigned
            "efficiency": 0,  # Efficiency would also be 0
            "total_duration": duration,  # Placeholder for total duration (e.g., 1 hour for idle time)
            "time_per_task": 0  # No tasks, so no time per task
         }])

         # Append this data to the DataFrame
         df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)

         print(f"Added Idle Data for {client_id}: {new_data.to_dict(orient='records')}")

def on_message(client, userdata, msg):
   """
   Callback when a message is received from the broker.
   """
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

   # Count messages on the results topic
   # Check if the client finished the task
   # only for analysis!!!
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
         client.publish("start_stop/taskWorker", 0, qos=1) # Status=0 when all clients worked the tasks
         client.publish("tasks_done", "done", qos=1) # Publish message to tg 
         
         print("handling idle clients..")
         duration = stop_distribution - start_distribution
         handle_idle_clients(duration)

         # change the n when more clients are connected!!
         aggregate_last_n_entries(len(connected_clients))
         task_count.clear() # set this to clear hear and not in end_task_session cause we need the values to handle idle clients
         
         # Do in-line comments if you dont need the logs
         # Directory where the script is located
         script_dir = os.path.dirname(os.path.realpath(__file__))  # get the directory of the python script

         # Create a "logged" directory within the script directory
         log_directory_power = os.path.join(script_dir, "power_logs_green")

         if not os.path.exists(log_directory_power):
               os.makedirs(log_directory_power, exist_ok=True)
         
         # print data to the csv file for doku
         timestamp = time.strftime("%Y-%m-%d %H-%M-%S")

         file = f"{timestamp}_power-log-random_{task_num}.csv"
         file_path = os.path.join(log_directory_power, file)
         print("Printing Power Data so CSV in Path:" + file_path)

         with write_to_power_log_lock:
            file = df_client_power.to_csv(file_path, index=False, encoding="utf-8")
            print("Created power log file.")

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
   # ONLY FOR ANALYSIS!!!
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
   client.will_set(f"status/{client_id}", "Disconnected", qos=1, retain=True)

   MQTT_Broker = get_broker_ip_via_file()
   Broker_Port = 1883

   try:
      # Connect to MQTT broker
      client.connect(MQTT_Broker, Broker_Port)
      client.enable_logger()

      # Start monitoring and tasks threads
      monitor_thread = threading.Thread(target=monitor_clients)
      task_thread = threading.Thread(target=distribute_tasks, args=(client,))
      monitor_thread.start()
      task_thread.start()

      client.loop_forever()

      # Publish initial messages
      client.publish(MQTT_Publish_Topic, f"This is the Manager. My name is {client_id} and I have subscribed to topic {MQTT_Publish_Topic}.")
      client.publish(MQTT_Result_Topic, f"This is the Manager. My name is {client_id} and I have subscribed to topic {MQTT_Result_Topic}.")

   except KeyboardInterrupt:
      print("Keyboard interrupt detected. Exiting gracefully...")
   except Exception as e:
      print("Caught Exception " + e)
   finally:
      stop_event.set()
      task_event.set()
      print("Set stop_event to False.")
      client.loop_stop()
      print("Stopped client loop.")
      client.disconnect()
      print("Client disconnected.")

      # Join threads if they are alive
      if 'monitor_thread' in locals() and monitor_thread.is_alive():
         monitor_thread.join(timeout=5)
      if 'task_thread' in locals() and task_thread.is_alive():
         task_thread.join(timeout=5)

      print("Threads joined. Exiting now.")
      sys.exit(0)

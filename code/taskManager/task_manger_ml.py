
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
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
import joblib

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
current_round = 1
max_round = 1
round_task_dict = {}
task_records = []
trained_model = None
model_loaded = False
model_available = False
ml_model = None
ml_encoder = None




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
   "round",
   "client_id", 
   "scenario", # Scenario is the name of the experiment, e.g. "apply", "create" or "refine"
   "total_power_usage", 
   "kwh",
   "relevant_power_values",
   "num_of_power_values", # how many values were collected during the process
   "tasks_assigned", 
   "efficiency_per_task", # power in watt per task
   "efficiency", # efficiency in tasks per kWh
   "total_duration", 
   "time_per_task",]

df_client_power = pd.DataFrame(columns=columns)

# This is needed when the client got no tasks
client_ids = [] #"444626", "283436", "854514", "943099", "956975"
idle_power_values = []  # Idle Power Values -> you have to collect them beforehand  11.36, 2.9, 11.315, 4.2, 72.7

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
    Initializes a new measurement series for a client for ONE task
    """
    # Power-Messung: leere Liste erzeugen oder anhängen
    if client_id not in power_tracking:
        power_tracking[client_id] = []
    # Task-Zähler initialisieren (optional – ggf. entfernen, falls du nur pro Runde zählst)
    if client_id not in task_count:
        task_count[client_id] = 0
    task_count[client_id] += 1

    # Zeitmessung initialisieren
    if client_id not in task_timing:
        task_timing[client_id] = []

    # Pro Task Startzeit und Runde merken
    task_timing[client_id].append({
        "start_time": time.time(),
        "round": current_round
    })


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

def end_single_task_session(client_id, scenario, end_time):
    global df_client_power

    if client_id not in power_tracking:
        print(f"No Power-Tracking for {client_id} found!")
        return

    if client_id not in task_timing or not task_timing[client_id]:
        print(f"No timing information for {client_id}.")
        return

    # Letzte Task-Zeit holen & entfernen
    timing = task_timing[client_id].pop()
    start_time = timing["start_time"]
    round_id = timing.get("round", current_round)  # Nutze aktuelle Runde wenn nicht anders gespeichert

    relevant_power_values = [
        power for timestamp, power in power_tracking[client_id]
        if start_time <= timestamp <= end_time
    ]

    if relevant_power_values:
        avg_power = float(np.mean(relevant_power_values))
    else:
        idle_power_value_list = df_idle_power.loc[df_idle_power['client_id'] == client_id, 'idle_power_value'].values
        avg_power = float(idle_power_value_list[0]) if len(idle_power_value_list) > 0 else 0.0

    total_duration = end_time - start_time
    
      # Summe aller vorherigen time_per_task-Einträge für diesen Client & Runde
    previous_tasks = df_client_power[
        (df_client_power["client_id"] == client_id) &
        (df_client_power["round"] == round_id)
    ]

    previous_time_sum = previous_tasks["time_per_task"].sum() if not previous_tasks.empty else 0
    time_per_task = (end_time - start_time) - previous_time_sum
    if time_per_task <= 0:
        time_per_task = total_duration  # Fallback falls was schiefgeht
        
    total_power_usage = avg_power * total_duration
    kwh = total_power_usage / 3600000

    new_data = pd.DataFrame([{
        "round": round_id,
        "client_id": client_id,
        "scenario": scenario,
        "total_power_usage": total_power_usage,
        "kwh": kwh,
        "avg_power": avg_power,
        "relevant_power_values": relevant_power_values,
        "num_of_power_values": len(relevant_power_values),
        "tasks_assigned": 1,
        "efficiency_per_task": kwh,
        "efficiency": 1 / kwh if kwh > 0 else 0,
        "total_duration": total_duration,
        "time_per_task": time_per_task,
    }])

    df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)
    print(f"📊 Einzelne Aufgabe gespeichert für {client_id}: {scenario}, Runde {round_id}")
    
    print(f"✅ Save Data for {client_id}: {new_data.to_dict(orient='records')}")
    
    task_records.append({
        "round": round_id,
        "client_id": client_id,
        "avg_power": avg_power,
        "total_duration": total_duration,
        "kwh": kwh,
        "efficiency": 1 / kwh if kwh > 0 else 0,
        "efficiency_per_task": kwh,
        "time_per_task": time_per_task,
        "task_features": {
            "scenario": scenario            
    }
    })



def end_task_session(client_id, end_time):

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

def aggregate_round_entries(round_number):
    global df_client_power

    # Filter für aktuelle Runde
    round_entries = df_client_power[df_client_power["round"] == round_number]

    if round_entries.empty:
        print(f"⚠️ Keine Einträge für Runde {round_number} gefunden.")
        return

    # Aggregation
    total_power = round_entries["total_power_usage"].sum()
    avg_power = total_power / len(round_entries)
    total_tasks = round_entries["tasks_assigned"].sum()
    total_power_values = round_entries["num_of_power_values"].sum()
    total_kwh = round_entries["kwh"].sum()

    avg_efficiency_per_task = round_entries["efficiency_per_task"].mean()
    avg_inv_efficiency = round_entries["efficiency"].mean()
    avg_time_per_task = round_entries["time_per_task"].mean()
    avg_duration = round_entries["total_duration"].max()

    # Neue Zeile mit Aggregatsdaten
    aggregated_row = pd.DataFrame([{
        "client_id": 0,
        "scenario": "",  # leer lassen
        "round": round_number,
        "total_power_usage": total_power,
        "avg_power": avg_power,
        "kwh": total_kwh,
        "relevant_power_values": None,
        "num_of_power_values": total_power_values,
        "tasks_assigned": total_tasks,
        "efficiency_per_task": avg_efficiency_per_task,
        "efficiency": avg_inv_efficiency,
        "total_duration": avg_duration,
        "time_per_task": avg_time_per_task
    }])

    # Anhängen
    df_client_power = pd.concat([df_client_power, aggregated_row], ignore_index=True)
    print(f"📊 Aggregierte Daten für Runde {round_number} hinzugefügt.")


def load_tasks_from_file():
  
    global round_task_dict
    global task_count
    global timestamp_file
    global finisher_counter
    global clients_with_tasks
    global task_num
    global max_round
    global current_round

    finisher_counter = 0
    clients_with_tasks = 0
    round_task_dict = {}
    current_round = 1
    task_num = 0

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
            loaded_tasks = [line.strip() for line in file.readlines() if line.strip()]

            for task in loaded_tasks:
                match = re.search(r'round=(\d+)', task)
                if match:
                    round_number = int(match.group(1))
                    if round_number == 1:
                        receiver = random.choice(list(connected_clients))
                    else:
                        receiver = predict_best_receiver(task, connected_clients)
                    
                    task_entry = f"{task} sender={client_id}, receiver={receiver}\""
                    round_task_dict.setdefault(round_number, []).append(task_entry)
                    task_num += 1
                else:
                    print(f"⚠️ Keine Runde in Task gefunden: {task}")

        print("Aufgaben erfolgreich geladen und nach Runden gruppiert.")

    except Exception as e:
        print(f"Fehler beim Laden von {task_file}: {e}")



def extract_features_from_task(task_string):
    """
    Extrahiert Szenario und andere relevante Features aus dem Task-String.
    Gibt ein Dictionary zurück.
    """
    features = {}

    scenario_match = re.search(r'scenario=([^,_]+)', task_string)
    features["scenario"] = scenario_match.group(1) if scenario_match else "unknown"

    return features


def train_receiver_model(df_power, task_records):
    """
    Trainiert ein Modell zur Vorhersage der besten Clients basierend auf Task-Effizienz.
    df_power: DataFrame mit Effizienz-Daten
    task_records: Liste von dicts mit Task-Features und zugeordnetem Client

    Speichert das Modell als 'receiver_model.pkl'
    """
    # Verbinde Task-Feature-Infos mit Power-Daten
    training_data = []
    for entry in task_records:
        client_id = entry["client_id"]
        task_info = entry["task_features"]

        power_entry = df_power[df_power["client_id"] == client_id].iloc[-1:]  # letzter Datensatz
        if power_entry.empty:
            continue

        features = {
            "scenario": task_info.get("scenario", "unknown"),
            "efficiency": power_entry["efficiency"].values[0],  # Tasks/kWh
        }
        training_data.append(features)

    df_train = pd.DataFrame(training_data)

    # One-Hot Encoding für kategorische Features
    enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    X = enc.fit_transform(df_train[["scenario"]])
    y = df_train["efficiency"].values

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)

    # Speichern
    joblib.dump((model, enc), "receiver_model.pkl")
    print("📈 ML-Modell gespeichert als 'receiver_model.pkl'")


def load_model_once():
    global model_loaded, model_available, ml_model, ml_encoder
    if model_loaded:
        return

    try:
        ml_model, ml_encoder = joblib.load("receiver_model.pkl")
        model_available = True
    except:
        print("⚠️ Kein trainiertes Modell vorhanden – verwende zufällige Auswahl.")
        model_available = False

    model_loaded = True



def predict_best_receiver(task_string, candidate_clients):
    
    global ml_model, ml_encoder

    """
    Nutzt das ML-Modell, um den besten Client aus der Liste vorherzusagen.
    """
    load_model_once()

    if not model_available:
        return random.choice(list(candidate_clients))

    features = extract_features_from_task(task_string)

    # Für logging/debugging
    known_scenarios = ml_encoder.categories_[0]
    if features["scenario"] not in known_scenarios:
        print(f"⚠️ Unbekanntes Szenario '{features['scenario']}' – wird ignoriert, aber Modell bleibt stabil.")

    X_input = pd.DataFrame([features])
    X_new = ml_encoder.transform(X_input)

    predicted_efficiency = ml_model.predict(X_new)[0]

    client_efficiencies = {}
    for client in candidate_clients:
        client_df = df_client_power[df_client_power["client_id"] == client]
        if client_df.empty:
            continue
        client_eff = client_df["efficiency"].iloc[-1]
        client_efficiencies[client] = abs(client_eff - predicted_efficiency)

    if client_efficiencies:
        return min(client_efficiencies, key=client_efficiencies.get)
    else:
        return random.choice(list(candidate_clients))



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
    global task_list
    global clients_with_tasks
    global task_count
    global start_distribution
    global current_round
    global round_task_dict

    while not stop_event.is_set():
        print("⏳ Warte auf neue Runde...")
        task_event.wait()  # Blockiert, bis Event gesetzt wird
        print(f"✅ Neue Runde erkannt (Runde {current_round}), beginne Verteilung...")

        if not connected_clients:
            print("⚠️ Keine verbundenen Clients. Warte...")
            time.sleep(5)
            continue

        with task_lock:
            # ⬇️ Lade die Aufgaben aus der aktuellen Runde
            task_list = round_task_dict.get(current_round, []).copy()

            if not task_list:
                print("⚠️ task_list ist leer, Event wird zurückgesetzt.")
                task_event.clear()
                continue

        local_clients_with_tasks = 0

        while True:
            with task_lock:
                if not task_list:
                    break
                task = task_list.pop(0)

            target_client = find_receiver(task)

            # Kombiniere alle Aufgaben für diesen Client
            combined_tasks = [task]
            i = 0
            while i < len(task_list):
                with task_lock:
                    next_task = task_list[i]
                    if find_receiver(next_task) == target_client:
                        combined_tasks.append(task_list.pop(i))
                    else:
                        i += 1

            task_string = "\n".join([
                re.sub(r"round=\d+,\s*", "", t)
                for t in combined_tasks
            ])

            if target_client in connected_clients:
                client.publish("start_stop/taskWorker", 1, qos=1)
                client.publish(f"tasks/{target_client}", task_string, qos=1)

                # 🛠️ WICHTIG: Für jede einzelne Aufgabe Startzeit registrieren
                for _ in combined_tasks:
                    start_task_session(target_client)

                task_count[target_client] = len(combined_tasks)
                local_clients_with_tasks += 1
                start_distribution = time.time()
                print(f"📦 Verteilte {len(combined_tasks)} Aufgaben an {target_client}")
            else:
                print(f"⚠️ Ziel-Client {target_client} nicht verbunden. Überspringe.")

            time.sleep(2)

        # ✅ Jetzt erst: Leere die Aufgaben der aktuellen Runde!
        with task_lock:
            round_task_dict[current_round] = []

        clients_with_tasks = local_clients_with_tasks
        task_event.clear()



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
    global task_list
    global task_count
    global finisher_counter
    global task_num
    global clients_with_tasks
    global stop_distribution
    global current_round
    global round_task_dict

    message = msg.payload.decode()
    topic = msg.topic

    # Debug-Ausgabe
    if not topic.startswith("ShellyVerbrauch"):
        print(f"Message received on {msg.topic}: {message}")

    # 🟨 Status-Nachrichten (Connected/Disconnected)
    if topic.startswith("status/"):
        client_name = topic.split("/")[1]

        # Ignoriere retained Nachrichten (z. B. alte Verbindungen)
        if msg.retain:
            print(f"⚠️ Ignoriere retained Nachricht für {client_name}")
            return

        if "Disconnected" in message:
            connected_clients.discard(client_name)
        elif "Connected" in message:
            connected_clients.add(client_name)
          # 🟦 Einzelne Task-Ergebnisse (laufen NICHT über finish/, sondern mqttTester/results)
          
    elif topic == "mqttTester/results":
        if "Task executed" in message and "scenario=" in message:
            print("📥 Eingehende Task-Ausführungs-Meldung:", message)

            match = re.match(r"(\w+): scenario=(\w+)_\w+", message)
            if match:
                client_id = match.group(1)
                scenario = match.group(2)
                end_time = time.time()
                end_single_task_session(client_id, scenario, end_time)


    # 🟩 Task-Finish-Meldungen
    elif topic.startswith("finish/"):
        finisher_counter += 1
        if message.startswith("Finished"):
            finished_client = message.split(" ")[1]
            client_status[finished_client] = 0
            end_time = time.time()
            #end_task_session(finished_client, end_time)
            
            
        if clients_with_tasks == finisher_counter:
            stop_distribution = time.time()
            print(f"✅ Runde {current_round} abgeschlossen: {finisher_counter}/{task_num} Tasks erledigt")

            client.publish("start_stop/taskWorker", 0, qos=1)
            client.publish("tasks_done", "done", qos=1)

            # Analyse und Logging
            duration = stop_distribution - start_distribution
            handle_idle_clients(duration)
            aggregate_round_entries(current_round)
            task_count.clear()

            # CSV-Export
            script_dir = os.path.dirname(os.path.realpath(__file__))
            log_directory_power = os.path.join(script_dir, "power_logs_ml")
            os.makedirs(log_directory_power, exist_ok=True)
            timestamp = time.strftime("%Y-%m-%d %H-%M-%S")
            file_path = os.path.join(log_directory_power, f"{timestamp}_power-log-random_{task_num}.csv")
            print("📁 Speichere Power-Log:", file_path)
            with write_to_power_log_lock:
                df_client_power.to_csv(file_path, index=False, encoding="utf-8")
                
            if len(task_records) >= 5:
                train_receiver_model(df_client_power, task_records)
            else:
                print("⚠️ Noch zu wenig Daten zum Trainieren des ML-Modells.")

            # ⬇️ Starte nächste Runde
            current_round += 1
            finisher_counter = 0
            clients_with_tasks = 0

            if current_round in round_task_dict and round_task_dict[current_round]:
                print(f"🚀 Starte Runde {current_round}")
                task_event.set()
            else:
                print("🎉 Alle Runden abgeschlossen.")

    # 🟦 Task-Generator aktiviert
    elif topic == "task_generator":
        print("⚙️ Task generator triggered. Lade Aufgaben...")
        load_tasks_from_file()

        if current_round in round_task_dict and round_task_dict[current_round]:
            print(f"🚀 Starte initiale Runde {current_round}")
            with task_lock:
                task_list = round_task_dict[current_round]
            clients_with_tasks = len(task_list)
            task_event.set()
        else:
            print("⚠️ Keine Aufgaben für Runde 1 gefunden.")

    # 🟫 Shelly Power-Daten (Analyse)
    elif topic.startswith("ShellyVerbrauch/") and "status" in topic and "switch:0" in topic:
        get_shelly_apower_data_status_switch(topic, message)
    elif topic.startswith("ShellyVerbrauch/") and "events" in topic:
        get_shelly_apower_data_events(topic, message)

    # 🔵 Client meldet sich per „My name is ...“
    match = re.search(r"My name is (\w+)", message)
    if match:
        connected_pc = match.group(1)
        connected_clients.add(connected_pc)


if __name__ == '__main__':
   client = mqtt.Client()
   client.on_connect = on_connect
   client.on_message = on_message
   client.username_pw_set(username=MQTT_Username, password=MQTT_Password)
   print(f"📋 Verbundene Clients bei Start: {connected_clients}")


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

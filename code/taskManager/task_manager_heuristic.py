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

connected_clients = set()          # Set aller verbundenen Clients
client_type = {}                   # client_type[cid] = "pi" oder "full"
clients_with_tasks = 0
current_round = 1
max_round = 1
client_task_done_counter = {}      # Wie viele Tasks ein Client abgeschlossen hat
client_idle_start_time = {}        # Wann ein Client idle geworden ist
round_task_dict = {}
task_records = []

# Greedy parameters
GREEDY_START_ROUND = 6             # Ab welcher Runde Greedy verwendet wird
SLIDING_WINDOW_SIZE = 5            # Nur die letzten 5 Runden betrachten
EPSILON = 0.05                     # Epsilon für gelegentliche Exploration

last_logged_random_round = 0
last_logged_greedy_round = 0

start_distribution = time.time()

stop_event = threading.Event()
task_event = threading.Event()

finisher_counter = 0  # Counter for received messages on results topic
task_num = 0          # Counter for loaded tasks

timestamp_file = None
log_lock = threading.Lock()        # Ensure logging 
write_to_power_log_lock = threading.Lock()

# Global task list
task_list = []
task_lock = threading.Lock()        # Ensure thread-safe access to task_list

client_status = defaultdict(int)    # 1: task distributed, 0: tasks completed

# Cache for current measured values per client
power_tracking = defaultdict(list)  # Stores all measured power values per client
task_count = defaultdict(int)       # Stores how many tasks a client has received

# Cache for start and end time per Client and Batch
task_timing = defaultdict(list)     # Stores start_time & round for each task

# DataFrame for consumption data
columns = [
   "round",
   "client_id", 
   "scenario", 
   "total_power_usage", 
   "kwh",
   "relevant_power_values",
   "num_of_power_values", 
   "tasks_assigned", 
   "efficiency_per_task", 
   "efficiency", 
   "total_duration", 
   "time_per_task",
]
df_client_power = pd.DataFrame(columns=columns)

# Idle-Power-Werte (vom Nutzer zu befüllen)
client_ids = []
idle_power_values = []
df_idle_power = pd.DataFrame({
    "client_id": client_ids,
    "idle_power_value": idle_power_values
})

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
    Initializes a new measurement series for a client for ONE task.
    """
    if client_id not in power_tracking:
        power_tracking[client_id] = []
    if client_id not in task_count:
        task_count[client_id] = 0
    task_count[client_id] += 1

    if client_id not in task_timing:
        task_timing[client_id] = []

    task_timing[client_id].append({
        "start_time": time.time(),
        "round": current_round
    })

def record_power_usage(client_id, power_value):
    """
    Saves individual power consumption values during processing.
    """
    power_tracking[client_id].append((time.time(), power_value))

def get_historical_mean_power_all_clients():
    average_last_avg_power = df_client_power.groupby("client_id")["avg_power"].mean()
    return average_last_avg_power.to_dict()

def get_historical_mean_power_one_client(client_id):
    historical_power_values = get_historical_mean_power_all_clients()
    return historical_power_values.get(client_id, None)

def end_single_task_session(client_id, scenario, end_time):
    """
    Called when a client reports a finished task; stores actual consumption to df_client_power.
    """
    global df_client_power, client_task_done_counter, client_idle_start_time

    if client_id not in power_tracking:
        print(f"No Power-Tracking for {client_id} found!")
        return

    if client_id not in task_timing or not task_timing[client_id]:
        print(f"No timing information for {client_id}.")
        return

    # Remove last timing entry
    timing = task_timing[client_id].pop()
    start_time = timing["start_time"]
    round_id = timing.get("round", current_round)

    relevant_power_values = [
        power for timestamp, power in power_tracking[client_id]
        if start_time <= timestamp <= end_time
    ]

    if relevant_power_values:
        avg_power = float(np.mean(relevant_power_values))
    else:
        idle_list = df_idle_power.loc[df_idle_power['client_id'] == client_id, 'idle_power_value'].values
        avg_power = float(idle_list[0]) if len(idle_list) > 0 else 0.0

    total_duration = end_time - start_time
    previous_tasks = df_client_power[
        (df_client_power["client_id"] == client_id) &
        (df_client_power["round"] == round_id)
    ]
    previous_time_sum = previous_tasks["time_per_task"].sum() if not previous_tasks.empty else 0
    time_per_task = total_duration - previous_time_sum
    if time_per_task <= 0:
        time_per_task = total_duration

    total_power_usage = avg_power * total_duration
    kwh = total_power_usage / 3600000

    client_task_done_counter[client_id] = client_task_done_counter.get(client_id, 0) + 1

    if (
        task_count.get(client_id, 0) > 0 and
        client_task_done_counter[client_id] == task_count[client_id] and
        finisher_counter < clients_with_tasks
    ):
        client_idle_start_time[client_id] = end_time
        print(f"🟡 Client {client_id} ist jetzt idle (alle Tasks erledigt)")

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
        "task_features": {"scenario": scenario}
    })

def end_task_session(client_id, end_time):
    """
    Alternative: When a client reports entire session finished. (Not used currently.)
    """
    global df_client_power, df_idle_power

    if client_id not in power_tracking:
        print(f"No Power-Tracking for {client_id} found!")
        return

    start_time = task_timing[client_id][-1]["start_time"]
    relevant_power_values = [
        power for timestamp, power in power_tracking[client_id]
        if start_time <= timestamp <= end_time
    ]

    if relevant_power_values:
        avg_power = float(np.mean(relevant_power_values))
    else:
        hist = get_historical_mean_power_one_client(client_id)
        if hist:
            avg_power = hist
        else:
            idle_list = df_idle_power.loc[df_idle_power['client_id'] == client_id, 'idle_power_value'].values
            avg_power = float(idle_list[0]) if len(idle_list) > 0 else 0.0

    total_duration = end_time - start_time
    total_power_usage = avg_power * total_duration
    kwh = total_power_usage / 3600000

    total_tasks = task_count.get(client_id, 0)
    inv_efficiency = total_tasks / kwh if kwh > 0 else 0
    efficiency_per_task = kwh / total_tasks if total_tasks > 0 else 0
    time_per_task = total_duration / total_tasks if total_tasks > 0 else 0

    new_data = pd.DataFrame([{
        "client_id": client_id,
        "total_power_usage": total_power_usage,
        "avg_power": avg_power,
        "kwh": kwh,
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
    del power_tracking[client_id]

def aggregate_round_entries(round_number):
    global df_client_power

    round_entries = df_client_power[df_client_power["round"] == round_number]
    if round_entries.empty:
        print(f"⚠️ Keine Einträge für Runde {round_number} gefunden.")
        return

    total_power = round_entries["total_power_usage"].sum()
    avg_power = total_power / len(round_entries)
    total_tasks = round_entries["tasks_assigned"].sum()
    total_power_values = round_entries["num_of_power_values"].sum()
    total_kwh = round_entries["kwh"].sum()

    avg_efficiency_per_task = round_entries["efficiency_per_task"].mean()
    avg_inv_efficiency = round_entries["efficiency"].mean()
    avg_time_per_task = round_entries["time_per_task"].mean()
    avg_duration = round_entries["total_duration"].max()

    aggregated_row = pd.DataFrame([{
        "client_id": 0,
        "scenario": "",
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
    df_client_power = pd.concat([df_client_power, aggregated_row], ignore_index=True)
    print(f"📊 Aggregierte Daten für Runde {round_number} hinzugefügt.")

def load_tasks_from_file():
    """
    Lädt alle Aufgaben aus generated_tasks.txt, gruppiert sie nach Runde.
    Hier wird noch kein Empfänger bestimmt.
    """
    global round_task_dict, task_count, timestamp_file
    global finisher_counter, clients_with_tasks, task_num, max_round, current_round

    finisher_counter = 0
    clients_with_tasks = 0
    round_task_dict = {}
    current_round = 1
    task_num = 0

    generator_dir = os.path.join(parent_dir, "taskGenerator")
    manager_dir = os.path.join(parent_dir, "taskManager")

    log_directory = os.path.join(manager_dir, 'logs')
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    task_file = os.path.join(generator_dir, "generated_tasks.txt")
    try:
        with open(task_file, 'r', encoding='utf-8') as file:
            loaded_tasks = [line.strip() for line in file.readlines() if line.strip()]
            for task in loaded_tasks:
                match = re.search(r'round=(\d+)', task)
                if match:
                    round_number = int(match.group(1))
                    task_entry = f"{task} sender={client_id}\""
                    round_task_dict.setdefault(round_number, []).append(task_entry)
                    task_num += 1
                else:
                    print(f"⚠️ Keine Runde in Task gefunden: {task}")
        print("Aufgaben erfolgreich geladen und nach Runden gruppiert.")
    except Exception as e:
        print(f"Fehler beim Laden von {task_file}: {e}")

def extract_features_from_task(task_string):
    """
    Extrahiert Szenario aus dem Task-String.
    """
    features = {}
    scenario_match = re.search(r'scenario=([^,_]+)', task_string)
    features["scenario"] = scenario_match.group(1) if scenario_match else "unknown"
    return features

def assign_task_to_client_greedy(task_string, candidate_clients, current_round):
    """
    Greedy-Algorithmus: wählt den Client mit minimalem kWh/s auf Basis der
    letzten SLIDING_WINDOW_SIZE Runden (inkl. IDLE).
    """
    scenario = extract_features_from_task(task_string)["scenario"]
    best_client = None
    best_metric = None
    # Sliding window: nur die letzten SLIDING_WINDOW_SIZE Runden betrachten
    window_start = max(1, current_round - SLIDING_WINDOW_SIZE)
    for c in candidate_clients:
        hist = df_client_power[
            (df_client_power["client_id"] == c) &
            (df_client_power["round"] >= window_start)
        ]
        if not hist.empty:
            total_kwh = hist["kwh"].sum()
            total_duration = hist["total_duration"].sum()
            metric = total_kwh / total_duration if total_duration > 0 else float('inf')
        else:
            metric = float('inf')  # Keine Daten => hoch setzen

        if best_metric is None or metric < best_metric:
            best_metric = metric
            best_client = c

    if best_client is None:
        best_client = random.choice(list(candidate_clients))

    print(f"⚙️ [Greedy] Runde {current_round} – Aufgabe '{scenario}' zugewiesen an {best_client} (kWh/s={best_metric:.6f})")
    return best_client

def find_receiver(task):
    """
    Extrahiert den in load_tasks_from_file noch nicht vergebenen receiver-Wert.
    """
    match = re.search(r'receiver=([^\s,]+)', task)
    if match:
        return match.group(1).rstrip('"')
    else:
        print("No receiver found")
        return None

def distribute_tasks(client):
    """
    Liest round_task_dict[current_round], filtert nach Pi/Full pro Task,
    wendet ε-Greedy oder Greedy an und sendet Tasks an die Clients.
    """
    global task_list, clients_with_tasks, task_count, start_distribution, current_round, round_task_dict
    global last_logged_random_round, last_logged_greedy_round

    while not stop_event.is_set():
        print("⏳ Warte auf neue Runde...")
        task_event.wait()
        print(f"✅ Neue Runde erkannt (Runde {current_round}), beginne Verteilung...")

        if not connected_clients:
            print("⚠️ Keine verbundenen Clients. Warte...")
            time.sleep(5)
            continue

        with task_lock:
            task_list = round_task_dict.get(current_round, []).copy()
            if not task_list:
                print("⚠️ task_list ist leer, Event wird zurückgesetzt.")
                task_event.clear()
                continue

        client_tasks = defaultdict(list)

        for task in task_list:
            round_match = re.search(r'round=(\d+)', task)
            round_number = int(round_match.group(1)) if round_match else current_round

            # Logging der Verteilstrategie
            if round_number < GREEDY_START_ROUND and last_logged_random_round != round_number:
                print(f"Runde {round_number} - Zufällige Verteilung")
                last_logged_random_round = round_number
            elif round_number >= GREEDY_START_ROUND and last_logged_greedy_round != round_number:
                print(f"Runde {round_number} - Greedy Verteilung (ε={EPSILON}, Fenster={SLIDING_WINDOW_SIZE})")
                last_logged_greedy_round = round_number

            # Kandidatenliste unter Berücksichtigung von Pi/Full
            scen = extract_features_from_task(task)["scenario"]
            valid_candidates = []
            only_pis = all(client_type.get(c, "full") == "pi" for c in connected_clients)

            if only_pis and scen != "apply":
                print(f"⚠️ Runde {round_number}: Nur Pis online, entferne Task '{scen}'.")
                continue

            for c in connected_clients:
                if client_type.get(c, "full") == "pi" and scen != "apply":
                    continue
                valid_candidates.append(c)

            if not valid_candidates:
                print(f"⚠️ Keine geeigneten Clients für Task '{scen}', Runde {round_number}.")
                continue

            # Empfänger bestimmen
            if round_number < GREEDY_START_ROUND:
                target_client = random.choice(valid_candidates)
            else:
                # ε-Greedy Exploration
                if random.random() < EPSILON:
                    target_client = random.choice(valid_candidates)
                    print(f"   → ε-Exploration: zufällig {target_client}")
                else:
                    target_client = assign_task_to_client_greedy(task, valid_candidates, round_number)

            # Empfänger in Task-String einfügen
            if "receiver=" not in task:
                task_with_receiver = task.rstrip('"') + f", receiver={target_client}\""
            else:
                task_with_receiver = re.sub(r'receiver=[^,"]*', f'receiver={target_client}', task)

            client_tasks[target_client].append(task_with_receiver)

        local_clients_with_tasks = 0
        for target_client, tasks in client_tasks.items():
            task_payload = "\n".join([
                re.sub(r"round=\d+,\s*", "", t) for t in tasks
            ])

            if target_client in connected_clients:
                client.publish("start_stop/taskWorker", 1, qos=1)
                client.publish(f"tasks/{target_client}", task_payload, qos=1)

                for _ in tasks:
                    start_task_session(target_client)

                task_count[target_client] = len(tasks)
                local_clients_with_tasks += 1
                start_distribution = time.time()
                print(f"📦 Verteilte {len(tasks)} Aufgaben an {target_client}")
            else:
                print(f"⚠️ Ziel-Client {target_client} nicht verbunden. Überspringe.")

            time.sleep(2)

        with task_lock:
            round_task_dict[current_round] = []

        clients_with_tasks = local_clients_with_tasks
        print(f"ℹ️ clients_with_tasks gesetzt auf {clients_with_tasks} nach Verteilung")
        task_event.clear()

def monitor_clients():
    """
    Gibt alle 10 Sekunden aus, welche Clients aktuell verbunden sind.
    """
    while not stop_event.is_set():
        print(f"Active clients: {connected_clients}")
        time.sleep(10)

def on_connect(client, userdata, flags, rc):
    """
    Sobald der Manager verbunden ist, abonniere alle relevanten Topics.
    """
    print("Connected with result code " + str(rc))
    connected_clients.clear()
    client.subscribe(MQTT_Publish_Topic, qos=0)
    client.subscribe(MQTT_Result_Topic, qos=0)
    client.subscribe("status/#")
    client.subscribe("task_generator", qos=1)
    client.subscribe("ShellyVerbrauch/#")
    client.subscribe("finish/#")

def get_shelly_apower_data_status_switch(topic, message):
    """
    Verarbeitung eingehender Shelly-Stromevents (Status mit 'apower').
    """
    client_id_json = topic.split("/")[1]
    if client_id_json in connected_clients:
        try:
            power_reading = json.loads(message)
            if "apower" in power_reading:
                actual_power = power_reading["apower"]
                if actual_power is not None:
                    record_power_usage(client_id_json, actual_power)
                    print(f"🔹 {client_id_json}: {actual_power} W")
                else:
                    print(f"No 'apower' data for {client_id_json}!")
            else:
                print(f"Messung ohne 'apower': {message}")
        except json.JSONDecodeError:
            print(f"Error parsing JSON: {message}")
        except Exception as e:
            print(f"Unexpected error when processing {topic}: {e}")

def get_shelly_apower_data_events(topic, message):
    """
    Verarbeitung eingehender Shelly-Stromevents (Events mit 'switch:0').
    """
    client_id_json = topic.split("/")[1]
    if client_id_json in connected_clients:
        try:
            power_reading = json.loads(message)
            if "params" in power_reading and "switch:0" in power_reading["params"]:
                actual_power = power_reading["params"]["switch:0"].get("apower")
                if actual_power is not None:
                    record_power_usage(client_id_json, actual_power)
                    print(f"🔹 {client_id_json}: {actual_power} W")
                else:
                    print(f"No 'apower' data for {client_id_json}!")
            else:
                print(f"Messung ohne relevante 'params': {message}")
        except json.JSONDecodeError:
            print(f"Error parsing JSON: {message}")
        except Exception as e:
            print(f"Unexpected error when processing {topic}: {e}")

def handle_idle_clients(duration, stop_time):
    """
    Erfasst Idle-Zeiten bei allen Clients (inkl. Pis) und erzeugt IDLE-Einträge im df.
    """
    global df_client_power, power_tracking, client_idle_start_time, task_count, connected_clients

    print(f"handle_idle_clients gestartet mit duration={duration:.2f}")
    client_ids_all = list(
        set(task_count.keys()) |
        set(client_idle_start_time.keys()) |
        set(connected_clients)
    )
    print(f"Clients insgesamt: {client_ids_all}")

    for cid in client_ids_all:
        is_unassigned = cid not in task_count or task_count[cid] == 0
        is_early_finisher = cid in client_idle_start_time
        if not is_unassigned and not is_early_finisher:
            continue

        if is_unassigned:
            idle_start = stop_time - duration
            idle_duration = duration
            print(f" → Client '{cid}' (nie beauftragt), idle_duration = gesamte Runde: {idle_duration:.2f}s")
        else:
            idle_start = client_idle_start_time[cid]
            idle_duration = stop_time - idle_start
            print(f" → Client '{cid}' früh fertig, idle_duration = {idle_duration:.2f}s")

        relevant_power_values = [
            power for ts, power in power_tracking.get(cid, [])
            if idle_start <= ts <= stop_time
        ]
        if relevant_power_values:
            avg_power = float(np.mean(relevant_power_values))
            num_values = len(relevant_power_values)
        else:
            avg_power = 0.0
            num_values = 0
            print(f"⚠️ Keine Leistungswerte für Idle‐Zeit von Client '{cid}' gefunden!")

        total_power_usage = avg_power * idle_duration
        kwh = total_power_usage / 3600000

        new_data = pd.DataFrame([{
            "round": current_round,
            "client_id": cid,
            "scenario": "IDLE",
            "total_power_usage": total_power_usage,
            "kwh": kwh,
            "relevant_power_values": relevant_power_values,
            "num_of_power_values": num_values,
            "tasks_assigned": 0,
            "efficiency_per_task": 0,
            "efficiency": 0,
            "total_duration": idle_duration,
            "time_per_task": 0
        }])
        df_client_power = pd.concat([df_client_power, new_data], ignore_index=True)
        print(f"➕ Idle‐Zeit erfasst für '{cid}': {idle_duration:.2f}s, Verbrauch: {kwh:.6f} kWh, Werte: {num_values}")
        print(f" → df_client_power Größe jetzt: {df_client_power.shape}")

    client_idle_start_time.clear()
    client_task_done_counter.clear()
    print("✅ handle_idle_clients beendet")

def on_message(client, userdata, msg):
    """
    Verarbeitet alle eingehenden MQTT-Nachrichten:
      - status/#           → verbundene Clients (inkl. Pi/Full)
      - mqttTester/results → Task-Ergebnis
      - finish/#           → Ende Runde → IDLE erfassen + aggregieren + nächsten Start
      - task_generator     → neue Runde laden
      - ShellyVerbrauch/#  → Stromdaten
      - 'My name is ... RPi=YES/NO' → Client meldet sich, Typ speichern
    """
    global task_list, task_count, finisher_counter, task_num, clients_with_tasks
    global stop_distribution, current_round, round_task_dict, start_distribution

    message = msg.payload.decode()
    topic = msg.topic

    if not topic.startswith("ShellyVerbrauch"):
        print(f"Message received on {msg.topic}: {message}")

    # ── Status/Disconnect ──
    if topic.startswith("status/"):
        client_name = topic.split("/")[1]
        if msg.retain:
            return
        if "Disconnected" in message:
            connected_clients.discard(client_name)
        elif "Connected" in message:
            connected_clients.add(client_name)

    # ── Task-Ergebnis ──
    elif topic == "mqttTester/results":
        if "Task executed" in message and "scenario=" in message:
            print("📥 Eingehende Task-Ausführungs-Meldung:", message)
            match = re.match(r"(\w+): scenario=(\w+)_\w+", message)
            if match:
                cid = match.group(1)
                scen = match.group(2)
                end_time = time.time()
                end_single_task_session(cid, scen, end_time)

    # ── finish/# ──
    elif topic.startswith("finish/"):
        finisher_counter += 1
        if message.startswith("Finished"):
            finished_client = message.split(" ")[1]
            client_status[finished_client] = 0

        if clients_with_tasks == finisher_counter:
            stop_distribution = time.time()
            print(f"✅ Runde {current_round} abgeschlossen: {finisher_counter}/{task_num} Tasks erledigt")
            client.publish("start_stop/taskWorker", 0, qos=1)
            client.publish("tasks_done", "done", qos=1)

            duration = stop_distribution - start_distribution
            handle_idle_clients(duration, stop_distribution)
            aggregate_round_entries(current_round)
            task_count.clear()

            # CSV-Export
            script_dir = os.path.dirname(os.path.realpath(__file__))
            log_directory_power = os.path.join(script_dir, "power_logs_heuristic")
            os.makedirs(log_directory_power, exist_ok=True)
            timestamp = time.strftime("%Y-%m-%d %H-%M-%S")
            file_path = os.path.join(log_directory_power, f"{timestamp}_power-log-heuristic_{task_num}.csv")
            print("📁 Speichere Power-Log:", file_path)
            with write_to_power_log_lock:
                df_client_power.to_csv(file_path, index=False, encoding="utf-8")

            # Nächste Runde starten
            current_round += 1
            finisher_counter = 0
            clients_with_tasks = 0

            next_round = current_round
            while next_round in round_task_dict and not round_task_dict[next_round]:
                next_round += 1

            if next_round in round_task_dict and round_task_dict[next_round]:
                current_round = next_round
                print(f"🚀 Starte Runde {current_round}")
                task_event.set()
            else:
                print("🎉 Alle Runden abgeschlossen.")

    # ── task_generator ──
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

    # ── Shelly Verbrauchsdaten ──
    elif topic.startswith("ShellyVerbrauch/") and "status" in topic and "switch:0" in topic:
        get_shelly_apower_data_status_switch(topic, message)
    elif topic.startswith("ShellyVerbrauch/") and "events" in topic:
        get_shelly_apower_data_events(topic, message)

    # ── Client meldet sich: “My name is … RPi=YES/NO” ──
    match = re.search(r"My name is (\w+)", message)
    if match:
        cid = match.group(1)
        connected_clients.add(cid)
        rpi_match = re.search(r"RPi=(YES|NO)", message)
        if rpi_match:
            client_type[cid] = "pi" if rpi_match.group(1) == "YES" else "full"
        else:
            client_type[cid] = "full"
        print(f"ℹ️ Client '{cid}' registriert als Typ '{client_type[cid]}'")
        return

if __name__ == '__main__':
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(username=MQTT_Username, password=MQTT_Password)
    print(f"📋 Verbundene Clients bei Start: {connected_clients}")

    client.will_set(f"status/{client_id}", "Disconnected", qos=1, retain=True)

    MQTT_Broker = get_broker_ip_via_file()
    Broker_Port = 1883

    try:
        client.connect(MQTT_Broker, Broker_Port)
        client.enable_logger()

        monitor_thread = threading.Thread(target=monitor_clients)
        task_thread = threading.Thread(target=distribute_tasks, args=(client,))
        monitor_thread.start()
        task_thread.start()

        client.loop_forever()

    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Exiting gracefully...")
    except Exception as e:
        print("Caught Exception " + str(e))
    finally:
        stop_event.set()
        task_event.set()
        client.loop_stop()
        client.disconnect()
        if 'monitor_thread' in locals() and monitor_thread.is_alive():
            monitor_thread.join(timeout=5)
        if 'task_thread' in locals() and task_thread.is_alive():
            task_thread.join(timeout=5)
        sys.exit(0)

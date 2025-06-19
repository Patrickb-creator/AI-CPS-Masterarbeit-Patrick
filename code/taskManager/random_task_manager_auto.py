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
client_task_done_counter = {}      # Wie viele Tasks ein Client abgeschlossen hat
client_idle_start_time = {}        # Wann ein Client idle geworden ist
current_round = 1
max_round = 1
round_task_dict = {}

start_distribution = time.time()

stop_event = threading.Event()
task_event = threading.Event()

finisher_counter = 0  # Zähler für finish-Meldungen
task_num = 0          # Zähler für insgesamt geladene Tasks

timestamp_file = None
log_lock = threading.Lock()
write_to_power_log_lock = threading.Lock()

# Global task list
task_list = []
task_lock = threading.Lock()

client_status = defaultdict(int)  # 1: Task verteilt, 0: erledigt

# Power-Tracking
power_tracking = defaultdict(list)
task_count = defaultdict(int)

# Timing pro Client/Task
task_timing = defaultdict(list)

# DataFrame für Verbrauchsdaten
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

# Idle-Power-Werte (muss vom Nutzer gepflegt werden)
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
    Initialisiert für eine neue Aufgabe bei client_id.
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
    Speichert eingehende Stromverbrauchswerte für client_id.
    """
    power_tracking[client_id].append((time.time(), power_value))


def get_historical_mean_power_one_client(client_id):
    avg = df_client_power.groupby("client_id")["avg_power"].mean()
    return avg.get(client_id, None)


def end_single_task_session(client_id, scenario, end_time):
    """
    Wird aufgerufen, wenn ein Client eine Task abgeschlossen meldet.
    Speichert die aktiven Verbrauchswerte in df_client_power.
    """
    global df_client_power, client_task_done_counter, client_idle_start_time

    if client_id not in power_tracking:
        print(f"No Power-Tracking for {client_id} found!")
        return
    if client_id not in task_timing or not task_timing[client_id]:
        print(f"No timing information for {client_id}.")
        return

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
        idle_list = df_idle_power.loc[df_idle_power["client_id"] == client_id, "idle_power_value"].values
        avg_power = float(idle_list[0]) if len(idle_list) > 0 else 0.0

    total_duration = end_time - start_time
    prev = df_client_power[
        (df_client_power["client_id"] == client_id) &
        (df_client_power["round"] == round_id)
    ]
    prev_time_sum = prev["time_per_task"].sum() if not prev.empty else 0
    time_per_task = total_duration - prev_time_sum
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


def end_task_session(client_id, end_time):
    """
    Alternative Methode, wenn der Client per finish/... seine gesamte Session
    als beendet meldet. (Optional, wird hier aktuell nicht aufgerufen.)
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
            idle_list = df_idle_power.loc[df_idle_power["client_id"] == client_id, "idle_power_value"].values
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
    """
    Am Ende einer Runde alle Einträge dieser Runde aggregieren.
    """
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
    Lädt alle Aufgaben aus generated_tasks.txt, gruppiert sie nach Runde,
    und weist jedem Task beim Speichern in round_task_dict bereits einen
    zufälligen "receiver" zu – mit Pi/Full-Logik.
    """
    global round_task_dict, task_count, finisher_counter, clients_with_tasks, task_num, current_round

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
                if not match:
                    print(f"⚠️ Keine Runde in Task gefunden: {task}")
                    continue

                round_number = int(match.group(1))
                # Features extrahieren (um Szenario herauszufinden)
                scen_match = re.search(r'scenario=([^\s,]+)', task)
                scen = scen_match.group(1).split('_')[0] if scen_match else "unknown"

                # Prüfen, ob nur Pis verbunden sind:
                only_pis = True
                for c in connected_clients:
                    if client_type.get(c, "full") != "pi":
                        only_pis = False
                        break

                # Wenn nur Pis und diese Task nicht "apply" ist → überspringen
                if only_pis and scen != "apply":
                    print(f"⚠️ Runde {round_number}: Nur Pis online, entferne Task '{scen}'.")
                    continue

                # Gültige Kandidatenliste zusammenstellen
                valid = []
                for c in connected_clients:
                    if client_type.get(c, "full") == "pi" and scen != "apply":
                        continue
                    valid.append(c)

                if not valid:
                    # Wenn valid leer – kein Full-Client und Task ist nicht "apply" oder keine Clients → überspringen
                    print(f"⚠️ Keine geeigneten Clients für Task '{scen}', Runde {round_number}.")
                    continue

                # Zufälligen Empfänger aus valid wählen
                receiver = random.choice(valid)
                task_entry = f"{task} sender={client_id}, receiver={receiver}\""
                round_task_dict.setdefault(round_number, []).append(task_entry)
                task_num += 1

        print("Aufgaben erfolgreich geladen und nach Runden gruppiert.")
    except Exception as e:
        print(f"Fehler beim Laden von {task_file}: {e}")


def find_receiver(task):
    """
    Extrahiert aus dem Task-String den bereits eingetragenen receiver.
    """
    match = re.search(r'receiver=([^\s,]+)', task)
    if match:
        return match.group(1).rstrip('"')
    else:
        print("No receiver found")
        return None


def distribute_tasks(client):
    """
    Liest round_task_dict[current_round] und sendet sie an die Clients.
    Da load_tasks_from_file die Empfänger bereits zufällig (unter Berücksichtigung
    von Pi/Full) bestimmt hat, muss hier nur noch das Publish erfolgen.
    """
    global task_list, clients_with_tasks, task_count, start_distribution, current_round, round_task_dict

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

        local_clients_with_tasks = 0

        # Wir nehmen jeden Task, ermitteln den receiver und schicken das Paket
        while True:
            with task_lock:
                if not task_list:
                    break
                task = task_list.pop(0)

            receiver = find_receiver(task)
            if receiver is None:
                continue

            # Alle Tasks für denselben receiver auf einmal bündeln
            combined_tasks = [task]
            i = 0
            while i < len(task_list):
                next_task = task_list[i]
                if find_receiver(next_task) == receiver:
                    combined_tasks.append(task_list.pop(i))
                else:
                    i += 1

            task_payload = "\n".join([
                re.sub(r"round=\d+,\s*", "", t) for t in combined_tasks
            ])

            if receiver in connected_clients:
                client.publish("start_stop/taskWorker", 1, qos=1)
                client.publish(f"tasks/{receiver}", task_payload, qos=1)

                for _ in combined_tasks:
                    start_task_session(receiver)

                task_count[receiver] = len(combined_tasks)
                local_clients_with_tasks += 1
                start_distribution = time.time()
                print(f"📦 Verteilte {len(combined_tasks)} Aufgaben an {receiver}")
            else:
                print(f"⚠️ Ziel-Client {receiver} nicht verbunden. Überspringe.")

            time.sleep(2)

        with task_lock:
            round_task_dict[current_round] = []

        # Clients-with-tasks als Anzahl der Clients, die Aufgaben bekamen
        # (sofern man überprüfen möchte, wie viele Clients wirklich rechnen)
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
    Sobald der Manager beim Broker connected ist, abonniere alle relevanten Topics.
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
    Erfasst Idle-Zeiten bei allen Clients (inkl. Pis), erzeugt IDLE-Einträge im df und
    leert anschließend client_idle_start_time sowie client_task_done_counter.
    """
    global df_client_power, power_tracking, connected_clients

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
        else:
            idle_start = client_idle_start_time[cid]
            idle_duration = stop_time - idle_start

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
      - status/#     → Connected/Disconnected
      - mqttTester/results → Task‐Ergebnis
      - finish/#     → wenn alle einer Runde fertig sind, IDLE erfassen, aggregieren, CSV‐Export
      - task_generator → neue Runde laden
      - ShellyVerbrauch/# → Strommessungen
      - 'My name is … RPi=YES/NO' → Client meldet sich + Typ
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
            print(f"✅ Runde {current_round} abgeschlossen: {finisher_counter}/{clients_with_tasks} Tasks erledigt")
            client.publish("start_stop/taskWorker", 0, qos=1)
            client.publish("tasks_done", "done", qos=1)

            duration = stop_distribution - start_distribution
            handle_idle_clients(duration, stop_distribution)
            aggregate_round_entries(current_round)
            task_count.clear()

            # CSV Export
            script_dir = os.path.dirname(os.path.realpath(__file__))
            log_directory_power = os.path.join(script_dir, "power_logs_random_final")
            os.makedirs(log_directory_power, exist_ok=True)
            timestamp = time.strftime("%Y-%m-%d %H-%M-%S")
            file_path = os.path.join(log_directory_power, f"{timestamp}_power-log-random_final{task_num}.csv")
            print("📁 Speichere Power-Log:", file_path)
            with write_to_power_log_lock:
                df_client_power.to_csv(file_path, index=False, encoding="utf-8")

            # Nächste Runde starten
            current_round += 1
            finisher_counter = 0
            clients_with_tasks = 0
            if current_round in round_task_dict and round_task_dict[current_round]:
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
            # Setze clients_with_tasks auf die gefilterte Anzahl an Tasks
            clients_with_tasks = len(task_list)
            print(f"ℹ️ clients_with_tasks = {clients_with_tasks} (nach Filterung)")
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

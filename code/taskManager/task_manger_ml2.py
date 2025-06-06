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
from sklearn.pipeline import make_pipeline
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
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
connected_clients = set()        # Set aller verbundenen Client-IDs
client_type = {}                 # client_type[cid] = "pi" oder "full"
clients_with_tasks = 0
current_round = 1
max_round = 1
round_task_dict = {}
task_records = []
client_task_done_counter = {}    # wie viele Tasks ein Client erledigt hat
client_idle_start_time = {}      # wann ein Client in den Idle-Status ging

# ML-Regressormodelle (werden nach jeder 5. Runde neu trainiert)
model_duration = None
model_kwh = None

start_distribution = time.time()
stop_event = threading.Event()
task_event = threading.Event()

finisher_counter = 0  # Counter für finish-Meldungen
task_num = 0          # Counter für insgesamt geladene Tasks

timestamp_file = None
log_lock = threading.Lock()
write_to_power_log_lock = threading.Lock()

# Global task list
task_list = []
task_lock = threading.Lock()

client_status = defaultdict(int)  # 1: Task verteilt, 0: fertig

# Power-Tracking
power_tracking = defaultdict(list)  # (timestamp, power) pro Client
task_count = defaultdict(int)       # Anzahl Tasks pro Client in aktueller Runde

# Timing pro Client/Task
task_timing = defaultdict(list)

# DataFrame für Verbrauchsdaten
columns = [
   "round",
   "client_id",
   "scenario",
   "knowledge_base",
   "activation_base",
   "code_base",
   "learning_base",
   "total_power_usage",
   "kwh",
   "avg_power",
   "relevant_power_values",
   "num_of_power_values",
   "tasks_assigned",
   "efficiency_per_task",
   "efficiency",
   "total_duration",
   "time_per_task",
]
df_client_power = pd.DataFrame(columns=columns)

# DataFrame für Idle-Power-Werte (pro Client, vordefiniert)
client_ids = list(set(task_count.keys()) | set(client_idle_start_time.keys()))
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
    """Markiere Beginn einer Task auf dem gegebenen client_id."""
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
    """Speichere jede ankommende Strommessung für client_id."""
    power_tracking[client_id].append((time.time(), power_value))


def get_historical_mean_power_one_client(client_id):
    avg = df_client_power.groupby("client_id")["avg_power"].mean()
    return avg.get(client_id, None)


def end_single_task_session(
    client_id, scenario, end_time,
    knowledge_base="-", activation_base="-", code_base="-", learning_base="-"
):
    """
    Wird aufgerufen, sobald eine einzelne Task (topic mqttTester/results)
    von client_id beendet gemeldet wird. Fügt die aktiven Verbrauchswerte
    hinzu und berechnet Avg-Power, kWh usw.
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

    # Alle Leistungswerte zwischen Start und End sammeln
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

    # Client-Task-Zähler inkrementieren
    client_task_done_counter[client_id] = client_task_done_counter.get(client_id, 0) + 1

    # Wenn dieser Client alle ihm zugewiesenen Tasks erfüllt hat:
    if (
        task_count.get(client_id, 0) > 0 and
        client_task_done_counter[client_id] == task_count[client_id] and
        finisher_counter < clients_with_tasks
    ):
        client_idle_start_time[client_id] = end_time
        print(f"🟡 Client {client_id} ist jetzt idle (alle Tasks erledigt)")

    # Daten Frame ergänzen
    new_data = pd.DataFrame([{
        "round": round_id,
        "client_id": client_id,
        "scenario": scenario,
        "knowledge_base": knowledge_base,
        "activation_base": activation_base,
        "code_base": code_base,
        "learning_base": learning_base,
        "total_power_usage": total_power_usage,
        "kwh": kwh,
        "avg_power": avg_power,
        "relevant_power_values": relevant_power_values,
        "num_of_power_values": len(relevant_power_values),
        "tasks_assigned": 1,
        "efficiency_per_task": kwh,
        "efficiency": (1 / kwh) if kwh > 0 else 0,
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
            "scenario": scenario,
            "knowledge_base": knowledge_base,
            "activation_base": activation_base,
            "code_base": code_base,
            "learning_base": learning_base
        }
    })


def aggregate_round_entries(round_number):
    """Am Ende einer Runde: alle Einträge dieser Runde aggregieren."""
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

    all_columns = set(df_client_power.columns)
    aggregated_data = {
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
    }
    for col in all_columns:
        if col not in aggregated_data:
            aggregated_data[col] = np.nan

    aggregated_row = pd.DataFrame([aggregated_data])
    df_client_power = pd.concat([df_client_power, aggregated_row], ignore_index=True)
    print(f"📊 Aggregierte Daten für Runde {round_number} hinzugefügt.")


def load_tasks_from_file():
    """
    Lädt die Datei generated_tasks.txt und befüllt round_task_dict,
    gruppiert nach 'round=…' aus den Aufgaben-Strings.
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
    Extrahiere scenario und Base-Features (knowledge/activation/code/learning) aus dem Task-String.
    Gibt Dictionary zurück.
    """
    features = {}
    scenario_match = re.search(r'scenario=([^\s,]+)', task_string)
    if scenario_match:
        scenario_full = scenario_match.group(1)
        features["scenario"] = scenario_full.split('_')[0]
    else:
        features["scenario"] = "unknown"
    def extract_base(key):
        match = re.search(rf'{key}=([^,]+)', task_string)
        return match.group(1).strip() if match else "-"
    features["knowledge_base"] = extract_base("knowledge_base")
    features["activation_base"] = extract_base("activation_base")
    features["code_base"] = extract_base("code_base")
    features["learning_base"] = extract_base("learning_base")
    return features


def train_task_models(df_power):
    """
    Trainiert zwei RandomForest-Regressoren:
     - model_duration: schätzt total_duration | (client_id, Task-Features)
     - model_kwh:     schätzt kwh           | (client_id, Task-Features)
    """
    global model_duration, model_kwh

    df = df_power.copy()
    # Nur echte Task-Zeilen: scenario != "IDLE", client_id != 0, tasks_assigned > 0
    df = df[(df["scenario"] != "IDLE") & (df["client_id"] != 0) & (df["tasks_assigned"] > 0)].copy()
    if df.empty:
        print("⚠️ Keine Trainingsdaten für Task-Modelle vorhanden.")
        return

    categorical = ["scenario", "knowledge_base", "activation_base", "code_base", "learning_base", "client_id"]

    # Dauer-Modell
    X_dur = df[categorical].copy()
    y_dur = df["total_duration"]
    preproc = ColumnTransformer(
        transformers=[("cat", OneHotEncoder(handle_unknown="ignore"), categorical)],
        remainder="passthrough"
    )
    model_duration = make_pipeline(
        preproc,
        RandomForestRegressor(n_estimators=100, random_state=42)
    )
    Xd_train, Xd_test, yd_train, yd_test = train_test_split(X_dur, y_dur, test_size=0.2, random_state=42)
    model_duration.fit(Xd_train, yd_train)
    pred_dur = model_duration.predict(Xd_test)
    print("⏱️ Dauer-Modell MSE:", mean_squared_error(yd_test, pred_dur))

    # kWh-Modell
    X_kwh = df[categorical].copy()
    y_kwh = df["kwh"]
    model_kwh = make_pipeline(
        preproc,
        RandomForestRegressor(n_estimators=100, random_state=42)
    )
    Xk_train, Xk_test, yk_train, yk_test = train_test_split(X_kwh, y_kwh, test_size=0.2, random_state=42)
    model_kwh.fit(Xk_train, yk_train)
    pred_kwh = model_kwh.predict(Xk_test)
    print("🔋 kWh-Modell MSE:", mean_squared_error(yk_test, pred_kwh))

    joblib.dump(model_duration, "model_duration.pkl")
    joblib.dump(model_kwh, "model_kwh.pkl")
    print("✅ Task-Modelle gespeichert als 'model_duration.pkl' und 'model_kwh.pkl'")


def load_models():
    """Lädt, sofern vorhanden, die bereits gespeicherten Regressoren."""
    global model_duration, model_kwh
    try:
        model_duration = joblib.load("model_duration.pkl")
        model_kwh = joblib.load("model_kwh.pkl")
        print("✅ Regressor-Modelle geladen.")
    except:
        print("⚠️ Keine gespeicherten Task-Modelle gefunden (verwende frisch trainieren).")


def schedule_tasks_greedy(task_strings, candidate_clients):
    """
    Zuteilung der übergebenen task_strings an candidate_clients so,
    dass der Gesamt-kWh-Verbrauch (aktiv + Idle) minimal wird.
    Greedy-Ansatz: iteriere Tasks, weise jedem
    Client zu, der inkrementellen Anstieg an kWh am geringsten verursacht.
    Pis werden für Non-apply-Szenarios per Schätzwert = inf ausgeschlossen.
    """
    # 1) Extrahiere Features für alle Tasks
    all_tasks = []
    for t in task_strings:
        feats = extract_features_from_task(t)
        all_tasks.append(feats)

    # 2) Für jeden Client und jede Task: Schätzung Dauer & kWh
    est_dur = {c: [] for c in candidate_clients}
    est_kwh = {c: [] for c in candidate_clients}
    for c in candidate_clients:
        for feats in all_tasks:
            scen = feats["scenario"]
            # Wenn c ein Pi ist und Task != "apply", dann setze Schätzer auf "inf"
            if client_type.get(c, "full") == "pi" and scen != "apply":
                est_dur[c].append(float("inf"))
                est_kwh[c].append(float("inf"))
            else:
                row = feats.copy()
                row["client_id"] = c
                X_row = pd.DataFrame([row])
                if model_duration is not None:
                    d = model_duration.predict(X_row)[0]
                    k = model_kwh.predict(X_row)[0]
                else:
                    # Falls kein Modell verfügbar, grobe Defaults
                    d = 10.0
                    k = 0.001
                est_dur[c].append(d)
                est_kwh[c].append(k)

    # 3) Greedy-Zuteilung basierend auf est_dur/est_kwh
    Assigned = {c: [] for c in candidate_clients}
    active_time = {c: 0.0 for c in candidate_clients}
    active_kwh = {c: 0.0 for c in candidate_clients}
    # Idle-Power-Werte pro Client
    idle_power = {
        c: float(df_idle_power.loc[df_idle_power["client_id"] == c, "idle_power_value"].values[0])
        if c in df_idle_power["client_id"].values else 0.0
        for c in candidate_clients
    }

    remaining = list(range(len(all_tasks)))
    round_len = 0.0

    while remaining:
        best_task, best_client, best_delta = None, None, float("inf")
        for i in remaining:
            scen = all_tasks[i]["scenario"]
            for c in candidate_clients:
                # Wenn Pi & Non-apply, überspringen (wir haben est = inf gesetzt)
                if client_type.get(c, "full") == "pi" and scen != "apply":
                    continue
                d_i = est_dur[c][i]
                k_i = est_kwh[c][i]
                if np.isinf(d_i) or np.isinf(k_i):
                    continue

                new_active_time_c = active_time[c] + d_i
                new_round_len = max(
                    round_len,
                    new_active_time_c,
                    max(active_time[d] for d in candidate_clients if d != c)
                )
                old_idle_c = round_len - active_time[c]
                new_idle_c = new_round_len - new_active_time_c
                delta_idle = new_idle_c - old_idle_c
                delta_idle_kwh = idle_power[c] * max(delta_idle, 0.0)
                delta_total = k_i + delta_idle_kwh

                if delta_total < best_delta:
                    best_delta = delta_total
                    best_task = i
                    best_client = c

        # Wechsle Task best_task zu best_client
        Assigned[best_client].append(best_task)
        active_time[best_client] += est_dur[best_client][best_task]
        active_kwh[best_client] += est_kwh[best_client][best_task]
        round_len = max(active_time.values())
        remaining.remove(best_task)

    # 4) Gib schließlich eine Map zurück: task_index → Client
    assignment = {}
    for c, tasks in Assigned.items():
        for i in tasks:
            assignment[i] = c
    return assignment


def distribute_tasks(client):
    """
    Verteilt die Tasks in round_task_dict[current_round] auf connected_clients.
    Abhängig davon, ob model_duration existiert (Runde ≥2 und nach erstem Modelltraining),
    wird entweder random (Runde 1–5) oder Greedy (ab Runde 6) verteilt.
    Pis werden nur für apply-Aufgaben berücksichtigt.
    """
    global task_list, clients_with_tasks, task_count, start_distribution, current_round, round_task_dict
    last_logged_round = 0

    while not stop_event.is_set():
        print("⏳ Warte auf neue Runde...")
        task_event.wait()  # Blockiert, bis event gesetzt
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

        # ----- Prüfen, ob ausschließlich Pis online sind -----
        only_pis = True
        for c in connected_clients:
            if client_type.get(c, "full") != "pi":
                only_pis = False
                break

        # Falls nur Pis online sind, entfernen wir alle create/refine-Tasks
        if only_pis:
            filtered = []
            for t in task_list:
                scen = extract_features_from_task(t)["scenario"]
                if scen == "apply":
                    filtered.append(t)
                else:
                    print(f"⚠️ Runde {current_round}: Nur Pis verfügbar → entferne Task '{scen}' (kann nicht ausgeführt werden).")
            task_list = filtered

        client_tasks = defaultdict(list)
        tasks = task_list.copy()

        # --- Runde 1..5 oder kein ML-Modell: Zufallsverteilung, aber Pis nur für apply ---
        if current_round == 1 or model_duration is None:
            if last_logged_round != current_round:
                print(f"Runde {current_round} - Zufällige Verteilung")
                last_logged_round = current_round

            for t in tasks:
                feats = extract_features_from_task(t)
                scen = feats["scenario"]

                # Kandidaten filtern: Wenn c ein Pi ist und scen != apply, skip
                geeignet = []
                for c in connected_clients:
                    if client_type.get(c, "full") == "pi" and scen != "apply":
                        continue
                    geeignet.append(c)

                if not geeignet:
                    # Falls kein Full-Client im Pool (alle sind Pis) – und task ist non-apply,
                    # dann haben wir task_list schon gefiltert (siehe oben),
                    # also hier nur apply-Tasks in "tasks" übrig. Sollte also nie passieren.
                    print(f"⚠️ Keine geeigneten Clients für Task '{scen}' → überspringe.")
                    continue

                target_client = random.choice(geeignet)
                # Empfängermarkierung wie gehabt anhängen
                if "receiver=" not in t:
                    t_with_receiver = t.rstrip('"') + f", receiver={target_client}\""
                else:
                    t_with_receiver = re.sub(r'receiver=[^,"]*', f"receiver={target_client}", t)
                client_tasks[target_client].append(t_with_receiver)

        # --- Runde ≥2 und ML-Modelle existieren: Greedy-Scheduling ---
        else:
            if last_logged_round != current_round:
                print(f"Runde {current_round} - ML Greedy-Verteilung")
                last_logged_round = current_round

            # Wenn ausschließlich Pis online sind, haben wir oben schon alle non-apply rausgeworfen.
            # schedule_tasks_greedy zieht anhand client_type automatisch Pis für apply, PCs für alles.
            assignment = schedule_tasks_greedy(tasks, list(connected_clients))

            for idx, t in enumerate(tasks):
                c = assignment[idx]
                if "receiver=" not in t:
                    t_with_receiver = t.rstrip('"') + f", receiver={c}\""
                else:
                    t_with_receiver = re.sub(r'receiver=[^,"]*', f"receiver={c}", t)
                client_tasks[c].append(t_with_receiver)

        # ----- Versand: Tasks pro Client bündeln und rauspublizieren -----
        local_clients_with_tasks = 0
        for target_client, tasks_for_client in client_tasks.items():
            # Wie im Original: führende "round=..," entfernen
            task_payload = "\n".join([
                re.sub(r"round=\d+,\s*", "", tt) for tt in tasks_for_client
            ])

            if target_client in connected_clients:
                client.publish("start_stop/taskWorker", 1, qos=1)
                client.publish(f"tasks/{target_client}", task_payload, qos=1)

                for _ in tasks_for_client:
                    start_task_session(target_client)

                task_count[target_client] = len(tasks_for_client)
                local_clients_with_tasks += 1
                print(f"📦 Verteilte {len(tasks_for_client)} Aufgaben an {target_client}")
            else:
                print(f"⚠️ Ziel-Client {target_client} nicht verbunden. Überspringe.")

            time.sleep(2)

        # Runde komplett verteilt → Reset
        with task_lock:
            round_task_dict[current_round] = []

        clients_with_tasks = local_clients_with_tasks
        start_distribution = time.time()
        task_event.clear()


def monitor_clients():
    """Periodisch ausgeben, welche Clients verbunden sind."""
    while not stop_event.is_set():
        print(f"Active clients: {connected_clients}")
        time.sleep(10)


def on_connect(client, userdata, flags, rc):
    """Sobald der Manager sich beim Broker verbindet, abonniere alle relevanten Topics."""
    print("Connected with result code " + str(rc))
    connected_clients.clear()
    client.subscribe(MQTT_Publish_Topic, qos=0)
    client.subscribe(MQTT_Result_Topic, qos=0)
    client.subscribe("status/#")
    client.subscribe("task_generator", qos=1)
    client.subscribe("ShellyVerbrauch/#")
    client.subscribe("finish/#")


def get_shelly_apower_data_status_switch(topic, message):
    """Verarbeite Shelly-Power-Meldungen (Status mit 'apower')"""
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
                print(f"Nachricht ohne 'apower': {message}")
        except json.JSONDecodeError:
            print(f"Error parsing JSON: {message}")
        except Exception as e:
            print(f"Unexpected error when processing {topic}: {e}")


def get_shelly_apower_data_events(topic, message):
    """Verarbeite Shelly-Power-Meldungen (Events mit 'switch:0')"""
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
                print(f"Nachricht ohne relevante 'params': {message}")
        except json.JSONDecodeError:
            print(f"Error parsing JSON: {message}")
        except Exception as e:
            print(f"Unexpected error when processing {topic}: {e}")


def handle_idle_clients(duration, stop_time):
    """
    Erfasst alle Clients (inkl. Pi) im Idle (= keine Tasks mehr), berechnet
    ihre Idle-Power über die Differenz stop_time - idle_start und schreibt
    die IDLE-Zeile in df_client_power.
    """
    global df_client_power, power_tracking, connected_clients

    client_ids_all = list(
        set(task_count.keys())
        | set(client_idle_start_time.keys())
        | set(connected_clients)
    )
    print(f"handle_idle_clients gestartet mit duration={duration:.2f}, stop_time={stop_time:.2f}")

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
            "knowledge_base": "-",
            "activation_base": "-",
            "code_base": "-",
            "learning_base": "-",
            "total_power_usage": total_power_usage,
            "avg_power": avg_power,
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
    client_idle_start_time.clear()
    client_task_done_counter.clear()


def on_message(client, userdata, msg):
    """
    Alle eingehenden MQTT-Nachrichten durchlaufen diesen Callback:
      - status/#  → Connected/Disconnected
      - mqttTester/results → Task-Ergebnis (speichern)
      - finish/#  → wenn alle Clients einer Runde fertig sind: Idle erfassen, aggregieren,
                    Modell evtl. nach jeder 5. Runde neu trainieren
      - task_generator → neue Aufgaben laden
      - ShellyVerbrauch/# → Stromdaten
      - "My name is … RPi=YES/NO" → Client meldet seine ID + ob Pi oder Full
    """
    global task_list, task_count, finisher_counter, task_num, clients_with_tasks
    global stop_distribution, current_round, round_task_dict, start_distribution

    message = msg.payload.decode()
    topic = msg.topic

    if not topic.startswith("ShellyVerbrauch"):
        print(f"Message received on {msg.topic}: {message}")

    # ────────────── status/# (Connected/Disconnected) ──────────────
    if topic.startswith("status/"):
        client_name = topic.split("/")[1]
        if msg.retain:
            return
        if "Disconnected" in message:
            connected_clients.discard(client_name)
        elif "Connected" in message:
            connected_clients.add(client_name)

    # ────────────── mqttTester/results (Task-Ergebnis) ──────────────
    elif topic == "mqttTester/results":
        if "Task executed" in message and "scenario=" in message:
            print("📥 Eingehende Task-Ergebnis-Meldung:", message)
            match = re.match(r"(\w+): scenario=(\w+)_\w+", message)
            if match:
                cid = match.group(1)
                scenario = match.group(2)
                end_time = time.time()

                def extract_base(key):
                    m = re.search(rf'{key}=([^,]+?)(?:,| - Task executed|$)', message)
                    return m.group(1).strip() if m else "-"

                knowledge_base = extract_base("knowledge_base")
                activation_base = extract_base("activation_base")
                code_base = extract_base("code_base")
                learning_base = extract_base("learning_base")

                end_single_task_session(
                    cid, scenario, end_time,
                    knowledge_base, activation_base, code_base, learning_base
                )

    # ────────────── finish/# (wenn alle Aufgaben einer Runde erledigt) ──────────────
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
            log_directory_power = os.path.join(script_dir, "power_logs_ml_2")
            os.makedirs(log_directory_power, exist_ok=True)
            timestamp = time.strftime("%Y-%m-%d %H-%M-%S")
            file_path = os.path.join(log_directory_power, f"{timestamp}_power-log-ml_2_{task_num}.csv")
            print("📁 Speichere Power-Log:", file_path)
            with write_to_power_log_lock:
                df_client_power.to_csv(file_path, index=False, encoding="utf-8")

            # Nur alle 5 Runden neu trainieren:
            if current_round % 5 == 0 and current_round >= 5:
                print(f"🔄 Runde {current_round} abgeschlossen ⇒ trainiere Task-Modelle auf Runden 1–{current_round}")
                train_task_models(df_client_power)
                load_models()
            else:
                print(f"ℹ️ Runde {current_round} abgeschlossen ⇒ kein Retraining (erst alle 5 Runden)")

            # Runde weiterschalten
            current_round += 1
            finisher_counter = 0
            clients_with_tasks = 0
            if current_round in round_task_dict and round_task_dict[current_round]:
                print(f"🚀 Starte Runde {current_round}")
                task_event.set()
            else:
                print("🎉 Alle Runden abgeschlossen.")

    # ────────────── task_generator (neue Aufgaben laden) ──────────────
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

    # ────────────── Shelly-Verbrauchsdaten ──────────────
    elif topic.startswith("ShellyVerbrauch/") and "status" in topic and "switch:0" in topic:
        get_shelly_apower_data_status_switch(topic, message)
    elif topic.startswith("ShellyVerbrauch/") and "events" in topic:
        get_shelly_apower_data_events(topic, message)

    # ────────────── Client meldet sich: “My name is … RPi=YES/NO” ──────────────
    match = re.search(r"My name is (\w+)", message)
    if match:
        cid = match.group(1)
        connected_clients.add(cid)
        # Prüfen, ob “RPi=YES” oder “RPi=NO” übergeben wurde:
        rpi_match = re.search(r"RPi=(YES|NO)", message)
        if rpi_match:
            if rpi_match.group(1) == "YES":
                client_type[cid] = "pi"
            else:
                client_type[cid] = "full"
        else:
            # Falls nicht explizit angegeben, standardmäßig “full”
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

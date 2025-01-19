import os
import socket
import subprocess
import logging
import paho.mqtt as mqtt
import shutil
import sys
import datetime

        # Logdatei und Konfiguration festlegen
broker_logfile = "mosquitto.log"
logging.basicConfig(
    filename=broker_logfile,            # Name der Logdatei
    level= logging.INFO,         # Loglevel (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format der Logs
    filemode="w",                     # Modus: "w" für überschreiben, "a" für anhängen
    # stream=sys.stdout # you have to decide wich logging you want, in stdout or in logfile
)

broker_ip_log = "broker_ip_log.txt"

def get_local_ip():
    # get the local hostname
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


def write_broker_ip():
    # Holen des aktuellen Datums und der Uhrzeit
    now = datetime.datetime.now()

    # Formatieren des Datums und der Uhrzeit (z.B. YYYY-MM-DD HH:MM:SS)
    formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")

    # get ip
    local_ip = get_local_ip()
    
    with open(broker_ip_log, "w") as file:
        # file.write(f"{formatted_now} - Broker IP: {local_ip}")
        file.write(f"{local_ip}")

def start_mqtt_broker_and_log():
    try:
        # config_path = "./messageBroker/mosquitto.conf"
         
        #if not os.path.exists(config_path):
         #   raise FileNotFoundError(f"Die Konfigurationsdatei '{config_path}' wurde nicht gefunden.")
       
        # start the broker
        # net stop mosquitto
        print(f"Starting the Broker...")

        write_broker_ip()
        print(f"Find the Broker IP in {broker_ip_log}")

        config_path = os.path.join(os.path.dirname(__file__), "mosquitto.conf")
        print(f"Config file path: {config_path}")

        process = subprocess.Popen(
            ["mosquitto", "-c", config_path], 
            # ["mosquitto",  "-v"], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True)
        
        print("Created log file.")
        print("Broker is now running. To terminate press CTRL+C")
        # comment in if you want to have all logs written in a log file
        # logging.info("Logs from Broker:\n")
      
        while True:
            output = process.stdout.readline()
            error_output = process.stderr.readline()
            if output == "" and process.poll() is not None and error_output == "":
                break
            if output:
                print(output.strip())  # Ausgabe in der Konsole
                logging.info(output.strip())
                logging.flush()  # Logdatei schreiben  
            if error_output:
                print(error_output.strip())  # Ausgabe der Fehler in der Konsole
                logging.error(error_output.strip())
    except FileNotFoundError:
        logging.error("Mosquitto wurde nicht gefunden. Ist es installiert und im PATH?")
        print("Fehler: Mosquitto wurde nicht gefunden. Bitte stelle sicher, dass es installiert ist.")
    except Exception as e:
        print(f"Error while starting the broker: {e}")
        logging.error(f"Error while starting the broker: {e}\n")

# Skript ausführen
if __name__ == "__main__":
    try:
        start_mqtt_broker_and_log()
    except KeyboardInterrupt:
        print("\nMQTT-Broker gestoppt.")
        logging.info("MQTT-Broker gestoppt.")
    
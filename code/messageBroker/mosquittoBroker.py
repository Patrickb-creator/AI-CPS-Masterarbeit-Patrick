"""
Starts a Mosquitto MQTT broker, logs its activity, and registers it on the local network via mDNS.

Features:
- Retrieves and logs the broker's local IP to a file.
- Registers the broker using Zeroconf as a `_mqtt._tcp.local.` service.
- Starts the Mosquitto broker using a local config file (`mosquitto.conf`).
- Logs broker output to both console and `broker_logs.log`.
- Handles graceful shutdown and mDNS service cleanup.

Requires:
- Mosquitto installed and in system PATH.
- `zeroconf` Python package.
"""

import os
import socket
import subprocess
import logging
import datetime
from zeroconf import ServiceInfo, Zeroconf

# Log file and config settings
broker_logfile = "mosquitto.log"

# logging to log in cmd and file, does not work properly..
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # log to cmd
        logging.FileHandler("broker_logs.log", mode="a")  # log to file "broker_logs.log"
    ]
)

broker_ip_log_path = os.path.dirname(os.path.abspath(__file__))
broker_ip_log = os.path.join(broker_ip_log_path, "broker_ip_log.txt")

def get_local_ip():
    """Get the local IP address of the current machine."""
    hostname = socket.gethostname()
    addr_info = socket.getaddrinfo(hostname, None)
    for addr in addr_info:
        if addr[0] == socket.AF_INET:  # IPv4
            return addr[4][0]

def write_broker_ip(local_ip):
    """Write the broker's IP address to a file."""
    now = datetime.datetime.now()
    formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
    
    with open(broker_ip_log, "w") as file:
        file.write(f"{local_ip}")

def register_mdns_service(local_ip):
    """Register the MQTT broker over mDNS (Zeroconf)."""
    hostname = socket.gethostname()
    service_name = f"{hostname}._mqtt._tcp.local."

    info = ServiceInfo(
        type_="_mqtt._tcp.local.",
        name=service_name,
        addresses=[socket.inet_aton(local_ip)],
        port=1883,
        properties={"description": "Mosquitto MQTT Broker"},
    )
    
    zeroconf = Zeroconf()
    zeroconf.register_service(info)
    logging.info(f"Broker registered as {service_name} at IP {local_ip}.")
    print(f"Broker registered as {service_name} at IP {local_ip}.")
    return zeroconf

def start_mqtt_broker_and_log():
    """Start the MQTT broker and register it via mDNS."""
    try:
        print("Starting the broker...")
        
        # Get local IP and write it to the log
        local_ip = get_local_ip()
        write_broker_ip(local_ip)
        print(f"Broker IP logged at {broker_ip_log}")
        
        # Register Zeroconf service
        zeroconf = register_mdns_service(local_ip)
        
        # Determine the configuration file path and start the broker
        config_path = os.path.join(os.path.dirname(__file__), "mosquitto.conf")
        
        if not os.path.exists(config_path):
            logging.error(f"Configuration file not found: {config_path}")
            return
        print(f"Configuration file path: {config_path}")

        process = subprocess.Popen(
            ["mosquitto", "-c", config_path, "-v"],  # -v for detailed Logging
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        print("Log file created. Broker is running. Press CTRL+C to terminate.")
      
        while True:
            output = process.stdout.readline()
            error_output = process.stderr.readline()
            if output == "" and process.poll() is not None and error_output == "":
                break
            if output:
                output_str = output.strip().decode("utf-8")  # Decrypt and Strip
                print(output_str)  # Show in CMD
                logging.info(output_str)  # Log to logfile
            if error_output:
                error_output_str = error_output.strip().decode("utf-8")
                print(error_output_str)  # Show error in CMD
                logging.error(error_output_str)  # Log error to logfile
    except FileNotFoundError:
        logging.error("Mosquitto not found. Is it installed and in PATH?")
        print("Error: Mosquitto not found. Please make sure it is installed.")
    except Exception as e:
        logging.error(f"Error while starting the broker: {e}")
        print(f"Error while starting the broker: {e}")
    finally:
        # Deregister Zeroconf service
        if 'zeroconf' in locals():
            zeroconf.close()

if __name__ == "__main__":
    try:
        start_mqtt_broker_and_log()
    except KeyboardInterrupt:
        print("\nMQTT broker stopped.")
        logging.info("MQTT broker stopped.")
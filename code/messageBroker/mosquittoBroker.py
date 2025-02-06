import os
import socket
import subprocess
import logging
import datetime
from zeroconf import ServiceInfo, Zeroconf

# log file and config settings
broker_logfile = "mosquitto.log"
logging.basicConfig(
    filename=broker_logfile,  # name of the log file
    level=logging.INFO,  # log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # log format
    filemode="w",  # mode: "w" for overwrite, "a" for append
)

broker_ip_log_path = os.path.dirname(os.path.abspath(__file__))
broker_ip_log = os.path.join(broker_ip_log_path, "broker_ip_log.txt")

def get_local_ip():
    """Get the local IP address of the current machine."""
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)

def write_broker_ip(local_ip):
    """Write the broker's IP address to a file."""
    now = datetime.datetime.now()
    formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
    
    with open(broker_ip_log, "w") as file:
        file.write(f"{local_ip}")

def register_mdns_service(local_ip):
    """Register the MQTT broker over mDNS (Zeroconf)."""
    service_name = "MQTT Broker._mqtt._tcp.local."
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
        print(f"Configuration file path: {config_path}")

        process = subprocess.Popen(
            ["mosquitto", "-c", config_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        print("Log file created. Broker is running. Press CTRL+C to terminate.")
      
        while True:
            output = process.stdout.readline()
            error_output = process.stderr.readline()
            if output == "" and process.poll() is not None and error_output == "":
                break
            if output:
                print(output.strip())
                logging.info(output.strip())
            if error_output:
                print(error_output.strip())
                logging.error(error_output.strip())
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
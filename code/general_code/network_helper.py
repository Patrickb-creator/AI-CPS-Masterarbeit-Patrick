""" 
Holds functions to help with the connections in the network. The functions are 
usually used by many different connectors so feel free to use this file
to get help with the connections
"""

import socket

# Get the latest broker ip of the broker which was started
def get_broker_ip():
   ip_file = "broker_ip_log.txt"
   try:
      with open(ip_file, 'r', encoding='utf-8') as file:
         broker_ip = file.read()

      print(broker_ip)
      return broker_ip
   except FileNotFoundError:
      print(f"File {ip_file} not found")

# Maybe you need the local ip adress of the computer on which a client is running
def get_local_ip():
    try:
        # Connection to a non-existent address to determine the network interface
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))  # 8.8.8.8 is an open DNS-Server from Google
            ip_address = s.getsockname()[0]  # Get own ip-adress
        return ip_address
    except Exception as e:
        print(f"Error when retrieving the IP address: {e}")
        return None
from zeroconf import ServiceBrowser, Zeroconf, ServiceListener
import time
import socket

class MQTTBrokerListener(ServiceListener):
    def __init__(self):
        self.broker_info = None

    def update_service(self, zeroconf, service_type, name):
        pass

    def remove_service(self, zeroconf, service_type, name):
        pass

    def add_service(self, zeroconf, service_type, name):
        info = zeroconf.get_service_info(service_type, name)
        if info:
            ip_address = socket.inet_ntoa(info.addresses[0])
            port = info.port
            self.broker_info = (ip_address, port)
            print(f"Discovered MQTT Broker at {ip_address}:{port}")


def discover_broker(service_type="_mqtt._tcp.local.", timeout=5):
    zeroconf = Zeroconf()
    listener = MQTTBrokerListener()
    browser = ServiceBrowser(zeroconf, service_type, listener)
    
    start_time = time.time()
    while listener.broker_info is None and (time.time() - start_time) < timeout:
        time.sleep(0.1)
    
    zeroconf.close()
    return listener.broker_info
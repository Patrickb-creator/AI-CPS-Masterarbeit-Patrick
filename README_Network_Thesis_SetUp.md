# Welcome to the setup guide for the distribution network developed as part of Lena Siegmund's master's thesis.

## Important Note:
To set up the AI-CPS Project so that the distribution strategies for the network work correctly, please follow the instructions provided by Marcus Grum on GitHub!!!

## Steps to set up the network:
1. Ensure that MQTT is installed and working on your device.

2. Configure the firewalls on all clients so that MQTT can be trusted, or temporarily deactivate the firewalls.

3. Start Docker on all devices.

4. Stop the MQTT Broker, which can be started as a background process when starting the device that should act as the broker.

5. On all devices: Create a Python virtual environment (venv) in the root folder of this project, activate it, and install all required packages. These include: paho-mqtt, zeroconf, pandas, numpy.

6. Start the broker by running mosquittoBroker.py on your broker device.

7. Start the Task Manager (the strategy you want to use). Choose either random_task_manager.py for random distribution or efficient_task_manager_v2.py for green distribution.

8. Now start the client devices. Activate the venv on all devices and run the code for the message clients: ai_simulation_enhanced.py for random distribution or ai_simulation_enhanced_green.py for the green strategy.

9. Start the Task Generator by running task_generator.py (for random task types) or task_generator_user_input.py (for specific task types) and follow the instructions provided in the terminal.

10. Please check the logs from time to time.

## Info:
When using the green strategy, Shelly Plugs are required to obtain power values for each client. Please ensure that you use those (configure them correctly and set the topics to ShellyVerbrauch/{client_id}) or other devices to supply power data to the manager. If you use other devices, you will need to adjust the code.

You can find the client IDs in the client_id.txt files on the devices.
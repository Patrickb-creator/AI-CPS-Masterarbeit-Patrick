# Welcome to the setup guide for the distribution network developed as part of Patrick Beyer´s master's thesis.

## Important Note:
To set up the AI-CPS Project so that the distribution strategies for the network work correctly, please follow the instructions provided by Marcus Grum on GitHub!!!

## Steps to set up the network:
1. Ensure that MQTT is installed and working on your device.

2. Configure the firewalls on all clients so that MQTT can be trusted, or temporarily deactivate the firewalls.

3. Start Docker on all devices.

4. Stop the MQTT Broker, which can be started as a background process when starting the device that should act as the broker.

5. On all devices: Create a Python virtual environment (venv) in the root folder of this project, activate it, and install all required packages. These include: paho-mqtt, zeroconf, pandas, numpy.

6. Start the broker by running mosquittoBroker.py on your broker device.

7. Start the Task Manager (the strategy you want to use). Choose either random_task_manager_auto.py for a fully automated random distribution, task_manager_heuristic.py for a fully automated greedy distribution or task_manger_ml.py for a machine learning distribution.

8. Now start the client devices. Activate the venv on all devices and run the code for the message clients: ai_simulation_enhanced.py for all distributions. 

## Important: In line 369 in ai_simulation_enhanced.py you need to set the expression "RPi=NO" or "RPi=YES" according to your client device. Raspberry PIs require the string "RPi=YES", all other devices use "RPi=NO". 

9. Start the Task Generator by running task_generator_auto.py. Enter the number of rounds and the number of tasks per round that you want to distribute. 

10. The logs are automatically safed after each round in a new folder. You can configure the path and the name of the folder inside the task manager code by using Visual Studi Code or other programs. 

## Info:
You need to configure the Shelly devices to gather the power values. Each Shelly requires the IP of your MQTT-Broker as well as the client ID of a device to track the power values.

You can find the client IDs in the client_id.txt files on the devices.
import subprocess
import paho.mqtt.client as mqtt
from multiprocessing import Process, Queue, current_process, freeze_support
from datetime import datetime

def build_docker_file_for_publication_at_dockerhub(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, logDirectory):
     """
     This functions builds docker file for ANN storage at Docker's hub.
     The file is stored at current working directory.
     - So far working for publishing knowledgeBase
     - Might be extended for publishing activationBase, codeBase and learningBase if required.
     """

     # if architecture = 'x86_64'
     if (hostArch == 'aarch64') or (hostArch == 'x86_64') or  (hostArch == 'x86_64_gpu'):
          with open(logDirectory+'/'+sender+'-docker-file', 'w') as f:
               f.write('# syntax=docker/dockerfile:1'+'\n')
               f.write('FROM busybox'+'\n')
               f.write(
                    'ADD ./'+sender+'_currentSolution.h5  /knowledgeBase/currentSolution.h5'+'\n')

def build_docker_compose_file_for_apply_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, logDirectory):
     """
     This functions builds docker-compose file for scenario called apply_annSolution
     and considers variables from message, here.
     The file is stored at current working directory.
     """

     # if architecture = 'x86_64'
     if (hostArch == 'x86_64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.0"'+'\n')
               f.write('services:'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_20
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  activation_base_'+sender+':\n') # e.g. marcusgrum/activationbase_apple_okay_01
               f.write('    image: ' + activation_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/activationBase/ && mkdir -p /tmp/' + sender+'/activationBase/ && cp -r /activationBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('      - "activation_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/apply_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'x86_64_gpu'
     if (hostArch == 'x86_64_gpu'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "2.3"  # the only version where "runtime" option is supported'+'\n')
               f.write('services:'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_20
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  activation_base_'+sender+':\n') # e.g. marcusgrum/activationbase_apple_okay_01
               f.write('    image: ' + activation_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/activationBase/ && mkdir -p /tmp/' + sender+'/activationBase/ && cp -r /activationBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64_gpu !!!!
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    # Make Docker create the container with NVIDIA Container Toolkit'+'\n')
               f.write('    # You do not need it if you set nvidia as the default runtime in'+'\n')
               f.write('    # daemon.json.'+'\n')
               f.write('    runtime: nvidia'+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('      - "activation_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/apply_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'aarch64'
     if (hostArch == 'aarch64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.9"'+'\n')
               f.write('services:'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_20
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  activation_base_'+sender+':\n') # e.g. marcusgrum/activationbase_apple_okay_01
               f.write('    image: ' + activation_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/activationBase/ && mkdir -p /tmp/' + sender+'/activationBase/ && cp -r /activationBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n')
               f.write('    user: root'+'\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_aarch64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('      - "activation_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/apply_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

def build_docker_compose_file_for_apply_annSolution_of_transportClassification(
          scenario, 
          knowledge_base, 
          activation_base, 
          code_base, 
          learning_base, 
          sender, 
          receiver, 
          hostArch, 
          logDirectory):
     """
     This functions builds docker-compose file for scenario called apply_annSolution
     and considers variables from message, here.
     The file is stored at current working directory.
     """

     # if architecture = 'x86_64' or if architecture = 'x86_64_gpu'
     if (hostArch == 'x86_64') or (hostArch == 'x86_64_gpu'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.0"'+'\n')
               f.write('services:'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_cps1_transport_system_01
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               # no activation base since activation base of previous ann activation is used
               #f.write('  activation_base_'+sender+':\n') # e.g. marcusgrum/activationbase_apple_okay_01
               #f.write('    image: ' + activation_base + ''+'\n')
               #f.write('    volumes:'+'\n')
               #f.write('       - ai_system:/tmp/'+''+'\n')
               #f.write('    command:'+'\n')
               #f.write('    - sh'+'\n')
               #f.write('    - "-c"'+'\n')
               #f.write('    - |'+'\n')
               #f.write('      rm -rf /tmp/'+sender+'/activationBase/ && mkdir -p /tmp/' + sender+'/activationBase/ && cp -r /activationBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_transport_classification_x86_64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               #f.write('      - "activation_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/apply_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'aarch64'
     if (hostArch == 'aarch64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.9"'+'\n')
               f.write('services:'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_cps1_transport_system_01
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n')
               f.write('    user: root'+'\n') # e.g. marcusgrum/codebase_ai_core_for_transport_classification_x86_64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/apply_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

def build_docker_compose_file_for_create_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, logDirectory):
     """
     This functions builds docker-compose file for scenario called create_annSolution
     and considers variables from message, here.
     The file is stored at current working directory.
     """

     # if architecture = 'x86_64'
     if (hostArch == 'x86_64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.0"'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/create_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'x86_64_gpu'
     if (hostArch == 'x86_64_gpu'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "2.3"  # the only version where "runtime" option is supported'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64_gpu !!!!
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    # Make Docker create the container with NVIDIA Container Toolkit'+'\n')
               f.write('    # You do not need it if you set nvidia as the default runtime in'+'\n')
               f.write('    # daemon.json.'+'\n')
               f.write('    runtime: nvidia'+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/create_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'aarch64'
     if (hostArch == 'aarch64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.9"'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n')
               f.write('    user: root'+'\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_aarch64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/create_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

def build_docker_compose_file_for_refine_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, logDirectory):
     """
     This functions builds docker-compose file for scenario called refine_annSolution
     and considers variables from message, here.
     The file is stored at current working directory.
     """

     # if architecture = 'x86_64'
     if (hostArch == 'x86_64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.0"'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_01
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/refine_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'x86_64_gpu'
     if (hostArch == 'x86_64_gpu'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "2.3"  # the only version where "runtime" option is supported'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_01
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64_gpu !!!!
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    # Make Docker create the container with NVIDIA Container Toolkit'+'\n')
               f.write('    # You do not need it if you set nvidia as the default runtime in'+'\n')
               f.write('    # daemon.json.'+'\n')
               f.write('    runtime: nvidia'+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/refine_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'aarch64'
     if (hostArch == 'aarch64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.9"'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_01
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n')
               f.write('    user: root'+'\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_aarch64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/refine_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

def build_docker_compose_file_for_evaluate_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, logDirectory):
     """
     This functions builds docker-compose file for scenario called evaluate_annSolution
     and considers variables from message, here.
     The file is stored at current working directory.
     """

     # if architecture = 'x86_64'
     if (hostArch == 'x86_64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.0"'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_01
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/evaluate_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'x86_64_gpu'
     if (hostArch == 'x86_64_gpu'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "2.3"  # the only version where "runtime" option is supported'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_01
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64_gpu !!!!
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    # Make Docker create the container with NVIDIA Container Toolkit'+'\n')
               f.write('    # You do not need it if you set nvidia as the default runtime in'+'\n')
               f.write('    # daemon.json.'+'\n')
               f.write('    runtime: nvidia'+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/evaluate_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'aarch64'
     if (hostArch == 'aarch64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.9"'+'\n')
               f.write('services:'+'\n')
               f.write('  learning_base_'+sender+':\n') # e.g. marcusgrum/learningbase_apple_banana_orange_pump_02
               f.write('    image: ' + learning_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/learningBase/ && mkdir -p /tmp/' + sender+'/learningBase/ && cp -r /learningBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  knowledge_base_'+sender+':\n') # e.g. marcusgrum/knowledgebase_apple_banana_orange_pump_01
               f.write('    image: ' + knowledge_base + ''+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/knowledgeBase/ && mkdir -p /tmp/' + sender+'/knowledgeBase/ && cp -r /knowledgeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('  code_base_'+sender+':\n')
               f.write('    user: root'+'\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_aarch64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    depends_on:'+'\n')
               f.write('      - "learning_base_'+sender+'"'+'\n')
               f.write('      - "knowledge_base_'+sender+'"'+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/evaluate_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

def build_docker_compose_file_for_wire_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, logDirectory):
     """
     This functions builds docker-compose file for scenario called wire_annSolution
     and considers variables from message, here.
     The file is stored at current working directory.
     """

     # if architecture = 'x86_64'
     if (hostArch == 'x86_64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.0"'+'\n')
               f.write('services:'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/wire_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'x86_64_gpu'
     if (hostArch == 'x86_64_gpu'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "2.3"  # the only version where "runtime" option is supported'+'\n')
               f.write('services:'+'\n')
               f.write('  code_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_x86_64_gpu !!!!
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    # Make Docker create the container with NVIDIA Container Toolkit'+'\n')
               f.write('    # You do not need it if you set nvidia as the default runtime in'+'\n')
               f.write('    # daemon.json.'+'\n')
               f.write('    runtime: nvidia'+'\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/wire_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

     # if architecture = 'aarch64'
     if (hostArch == 'aarch64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.9"'+'\n')
               f.write('services:'+'\n')
               f.write('  code_base_'+sender+':\n')
               f.write('    user: root'+'\n') # e.g. marcusgrum/codebase_ai_core_for_image_classification_aarch64
               f.write('    image: ' + code_base + '_' + hostArch + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      rm -rf /tmp/'+sender+'/codeBase/ && mkdir -p /tmp/' + sender+'/codeBase/ && cp -r /codeBase/ /tmp/'+sender+'/;'+'\n')
               f.write('      python3 /tmp/'+sender + '/codeBase/wire_annSolution.py ' + sender + " " + receiver + ';\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

def build_docker_compose_file_for_manual_sensorValueUpdate(
          sender, 
          cps1_conveyor_workpieceSensorLeft, 
          cps1_conveyor_workpieceSensorCenter, 
          cps1_conveyor_workpieceSensorRight, 
          cps2_conveyor_workpieceSensorLeft, 
          cps2_conveyor_workpieceSensorCenter, 
          cps2_conveyor_workpieceSensorRight,
          hostArch, 
          logDirectory):
     """
     This functions builds docker-compose file for scenario called manual_sensorValueUpdate
     and considers variables from message, here.
     The file is stored at current working directory.
     """

     # if architecture = 'x86_64' or if architecture = 'x86_64_gpu' or if architecture = 'aarch64'
     if (hostArch == 'x86_64') or (hostArch == 'x86_64_gpu') or (hostArch == 'aarch64'):
          with open(logDirectory+'/'+sender+'-docker-compose.yml', 'w') as f:
               f.write('version: "3.0"'+'\n')
               f.write('services:'+'\n')
               f.write('  sensor_base_'+sender+':\n') # e.g. marcusgrum/codebase_ai_core_for_transport_classification_x86_64
               f.write('    image: busybox' + '\n')
               f.write('    volumes:'+'\n')
               f.write('       - ai_system:/tmp/'+''+'\n')
               f.write('    command:'+'\n')
               f.write('    - sh'+'\n')
               f.write('    - "-c"'+'\n')
               f.write('    - |'+'\n')
               f.write('      mkdir -p /tmp/' + sender + '/activationBase/currentActivation/;\n')
               f.write('      echo ' + cps1_conveyor_workpieceSensorLeft   + ' > /tmp/' + sender + '/activationBase/currentActivation/cps1_conveyor_workpieceSensorLeft.txt;\n')
               f.write('      echo ' + cps1_conveyor_workpieceSensorCenter + ' > /tmp/' + sender + '/activationBase/currentActivation/cps1_conveyor_workpieceSensorCenter.txt;\n')
               f.write('      echo ' + cps1_conveyor_workpieceSensorRight  + ' > /tmp/' + sender + '/activationBase/currentActivation/cps1_conveyor_workpieceSensorRight.txt;\n')
               f.write('      echo ' + cps2_conveyor_workpieceSensorLeft   + ' > /tmp/' + sender + '/activationBase/currentActivation/cps2_conveyor_workpieceSensorLeft.txt;\n')
               f.write('      echo ' + cps2_conveyor_workpieceSensorCenter + ' > /tmp/' + sender + '/activationBase/currentActivation/cps2_conveyor_workpieceSensorCenter.txt;\n')
               f.write('      echo ' + cps2_conveyor_workpieceSensorRight  + ' > /tmp/' + sender + '/activationBase/currentActivation/cps2_conveyor_workpieceSensorRight.txt;\n')
               f.write('volumes:'+'\n')
               f.write('  ai_system:'+'\n')
               f.write('    external: true'+'\n')

def unroll_sensorValuesFromScenario(message):
     """
     This functions unrolls variables from message and returns them.
     """

     scenario                            = message.partition(", cps1_conveyor_workpieceSensorLeft=")[0]
     cps1_conveyor_workpieceSensorLeft   = (message.partition("cps1_conveyor_workpieceSensorLeft=")[2]).partition(", cps1_conveyor_workpieceSensorCenter=")[0]
     cps1_conveyor_workpieceSensorCenter = (message.partition("cps1_conveyor_workpieceSensorCenter=")[2]).partition(", cps1_conveyor_workpieceSensorRight=")[0]
     cps1_conveyor_workpieceSensorRight  = (message.partition("cps1_conveyor_workpieceSensorRight=")[2]).partition(", cps2_conveyor_workpieceSensorLeft=")[0]
     cps2_conveyor_workpieceSensorLeft   = (message.partition("cps2_conveyor_workpieceSensorLeft=")[2]).partition(", cps2_conveyor_workpieceSensorCenter=")[0]
     cps2_conveyor_workpieceSensorCenter = (message.partition("cps2_conveyor_workpieceSensorCenter=")[2]).partition(", cps2_conveyor_workpieceSensorRight=")[0]
     cps2_conveyor_workpieceSensorRight  = (message.partition("cps2_conveyor_workpieceSensorRight=")[2]).partition(", knowledge_base=")[0]
     
     return scenario, cps1_conveyor_workpieceSensorLeft, cps1_conveyor_workpieceSensorCenter, cps1_conveyor_workpieceSensorRight, cps2_conveyor_workpieceSensorLeft, cps2_conveyor_workpieceSensorCenter, cps2_conveyor_workpieceSensorRight

def run_docker_compose_parallel(sender, receiver, log_directory, log_to_file=True):
     """
     Runs a Docker Compose setup asynchronously for the given sender, with optional logging.
     """
     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Paths to log files
     stdout_file = f"{log_directory}/{sender}_{timestamp}_stdout.txt"
     stderr_file = f"{log_directory}/{sender}_{timestamp}_stderr.txt"

    # Create log files, if user wants to have them
     if log_to_file:
          stdout_stream = open(stdout_file, "wb")
          stderr_stream = open(stderr_file, "wb")
     else:
          stdout_stream = subprocess.PIPE
          stderr_stream = subprocess.PIPE
     try:
          # Start Docker Compose
          p = subprocess.Popen(
               f"docker-compose -f {log_directory}/{sender}-docker-compose.yml up --remove-orphans",
               shell=True, stdout=stdout_stream, stderr=stderr_stream
               )
        
          print(f'Message of {sender} has been triggered at {receiver} successfully!')
        
          # Catch the output after it´s done
          stdout, stderr = p.communicate()
     
          # Show results directly in console when file logging is deactivated
          if not log_to_file:
               if stdout:
                    print(stdout.decode('utf-8'))
               if stderr:
                    print(stderr.decode('utf-8'))
          else:
               if stdout:
                    with open(stdout_file, "ab") as f:
                         f.write(stdout)
               if stderr:
                    with open(stderr_file, "ab") as f:
                         f.write(stderr)
     finally:      
          if log_to_file:
               stdout_stream.close()
               stderr_stream.close()


def run_docker_compose_sequential(sender, receiver, log_directory, log_to_file=True):
     """
     Runs a Docker Compose setup synchronously and waits for it to finish, with optional logging.
     """
     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Paths to log files
     stdout_file = f"{log_directory}/{sender}_{timestamp}_stdout.txt"
     stderr_file = f"{log_directory}/{sender}_{timestamp}_stderr.txt"

    # Create log files, if user wants to have them
     if log_to_file:
          stdout_stream = open(stdout_file, "wb")
          stderr_stream = open(stderr_file, "wb")
     else:
          stdout_stream = subprocess.PIPE
          stderr_stream = subprocess.PIPE
     try:
          # Code is waiting until docker really finished! so we dont need a ressource manager
          p = subprocess.run(
               f"docker-compose -f {log_directory}/{sender}-docker-compose.yml up --remove-orphans",
               shell=True, stdout=stdout_stream, stderr=stderr_stream
               )
        
          print(f'Message of {sender} has been triggered at {receiver} successfully!')
          
          # Use stdout and stderr directly from the process p
          if p.stdout:
              if log_to_file:
                  with open(stdout_file, "ab") as f:
                      f.write(p.stdout)
              else:
                  print(p.stdout.decode('utf-8'))

          if p.stderr:
              if log_to_file:
                  with open(stderr_file, "ab") as f:
                      f.write(p.stderr)
              else:
                  print(p.stderr.decode('utf-8'))
                    # Enhancement: set_power_scheme("381b4222-f694-41f0-9685-ff5bb260df2e")
                    # Enhancement: print("Set power scheme to balanced.")
     finally:         
          if log_to_file:
               stdout_stream.close()
               stderr_stream.close()

def realize_scenario(
          log_directory, 
          MQTT_Topic_Results,
          scenario, 
          knowledge_base, 
          activation_base,
          code_base, 
          learning_base, 
          client, 
          sender, 
          receiver, 
          hostName, 
          hostArch,
          sub_process_method):
     """
     This function realizes scenarios, such as from communication client
     and manages the corresponding AI reguests.
     """
     # clear_log_directory(log_directory)
     
     # build docker-compose file based on message
     # for standard situations (experiment01-04)
     if (scenario == 'apply_annSolution'):
          build_docker_compose_file_for_apply_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, log_directory)
     if (scenario == 'create_annSolution'):
          build_docker_compose_file_for_create_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, log_directory)
     if (scenario == 'refine_annSolution'):
          build_docker_compose_file_for_refine_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, log_directory)
     if (scenario == 'wire_annSolution'):
          build_docker_compose_file_for_wire_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, log_directory)
     if (scenario == 'evaluate_annSolution'):
          build_docker_compose_file_for_evaluate_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, log_directory)

     # for experiment05
     if (scenario == 'apply_annSolution_for_imageClassification'):
          build_docker_compose_file_for_apply_annSolution(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver, hostArch, log_directory)
     if (scenario == 'apply_annSolution_for_transportClassification'):
          build_docker_compose_file_for_apply_annSolution_of_transportClassification(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver,hostArch, log_directory)
     if ('manual_sensorValueUpdate' in scenario):
          scenario, cps1_conveyor_workpieceSensorLeft, cps1_conveyor_workpieceSensorCenter, cps1_conveyor_workpieceSensorRight, cps2_conveyor_workpieceSensorLeft, cps2_conveyor_workpieceSensorCenter, cps2_conveyor_workpieceSensorRight = unroll_sensorValuesFromScenario(scenario)
          build_docker_compose_file_for_manual_sensorValueUpdate(sender, cps1_conveyor_workpieceSensorLeft, cps1_conveyor_workpieceSensorCenter, cps1_conveyor_workpieceSensorRight, cps2_conveyor_workpieceSensorLeft, cps2_conveyor_workpieceSensorCenter, cps2_conveyor_workpieceSensorRight, hostArch, log_directory)

     # realize instructions from messages by running docker-compose file created at machine-specific working directory
     if (scenario == 'apply_annSolution') or (scenario == 'create_annSolution') or (scenario == 'refine_annSolution') or (scenario == 'wire_annSolution') or (scenario == 'evaluate_annSolution'):

          if (sub_process_method == "sequential"):
               # a) by subprocess.run() or by subprocess.call() [depreciated]
               # Remark: By this variant, parallel requests at the same machine are realized sequentially, which is managed by message broaker (next request is delivered when previous request has been finished).
               #         So, requests are realized one after the other.
               # subprocess.call("docker-compose -f "+logDirectory+"/"+sender+"-docker-compose.yml up --remove-orphans", shell=True)
               run_docker_compose_sequential(sender, receiver, log_directory, log_to_file=True)

          if (sub_process_method == "parallel"):
               # b) by subprocess.Popen()
               # Remark: By this variant, parallel requests at the same machine are realized in parallel. Hence, individual stdout and stderr have been created so that CLI output is separated correctly.
               # Please note, message broaker does not manage requests. Indeed, each machine requires a manager for efficient ressource allocation.
               try:
                    run_docker_compose_parallel(sender, receiver, log_directory, log_to_file=True)
                    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    # with open(log_directory+"/"+sender+ "_" + timestamp + "_stdout.txt", "wb") as out, \
                    #      open(log_directory+"/"+sender+ "_" + timestamp + "_stderr.txt", "wb") as err:

                    #      # carry out current scenario
                    #      p = subprocess.Popen(
                    #           "docker-compose -f "+ log_directory + "/" + sender + "-docker-compose.yml up --remove-orphans", shell=True, stdout=out, stderr=err)
                    #      print('Message of ' + sender + ' has been triggered at ' + receiver + ' successfully!')

                    #      stdout, stderr = p.communicate()

                    #      if stdout:
                    #           with open(f"{log_directory}/{sender}_stdout.txt", "a") as f:
                    #                f.write(stdout.decode('utf-8'))
                    #      if stderr:
                    #           with open(f"{log_directory}/{sender}_stderr.txt", "a") as f:
                    #                f.write(stderr.decode('utf-8'))
                    client.publish(MQTT_Topic_Results, hostName + ': Docker compose ran')                                                   
               except Exception as e:
                    print(f"Fehler beim Ausführen des Szenarios: {str(e)}")
                    client.publish(MQTT_Topic_Results, hostName + ': This is an error! I could not process the ann request!')                                                   
                    

     # If new knowledgeBase needs to be published to docker's hub, when create or refine scenarios have been finalized:
     if (scenario == 'publish_annSolution'):
          # 1. specify docker file for knowledgeBase (for preparing publication to docker's hub)
          build_docker_file_for_publication_at_dockerhub(scenario, knowledge_base, activation_base, code_base, learning_base, sender, receiver)

          if (sub_process_method == "sequential"):
               # 2. copy ANN to dockers current build context folder (for preparing publication to docker's hub)
               subprocess.run("docker run --rm -v $PWD/logs:/host -v ai_system:/ai_system -w /ai_system busybox cp /ai_system/" + sender+"/knowledgeBase/currentSolution.h5 /host/"+sender+"_currentSolution.h5", shell=True)
               # 3. build ANN-based container for relevant architectures in current build context folder and publish at docker's hub
               subprocess.run("docker buildx build --platform linux/arm/v7,linux/arm64/v8,linux/amd64 --file "+log_directory+"/"+sender+"-docker-file --tag marcusgrum/knowledgebase_"+sender+":latest --push  "+log_directory+"/", shell=True)

          if (sub_process_method == "parallel"):
               # 2. copy ANN to dockers current build context folder (for preparing publication to docker's hub)
               with open(log_directory+"/"+sender+"_stdout.txt", "wb") as out, open(log_directory+"/"+sender+"_stderr.txt", "wb") as err:
                    subprocess.Popen("docker run --rm -v $PWD/logs:/host -v ai_system:/ai_system -w /ai_system busybox cp /ai_system/"+sender+"/knowledgeBase/currentSolution.h5 /host/"+sender+"_currentSolution.h5", shell=True, stdout=out, stderr=err)
               # 3. build ANN-based container for relevant architectures in current build context folder and publish at docker's hub
               with open(log_directory+"/"+sender+"_stdout.txt", "wb") as out, open(log_directory+"/"+sender+"_stderr.txt", "wb") as err:
                    subprocess.Popen("docker buildx build --platform linux/arm/v7,linux/arm64/v8,linux/amd64 --file "+log_directory+"/"+sender+"-docker-file --tag marcusgrum/knowledgebase_"+sender+":latest --push  "+log_directory+"/", shell=True, stdout=out, stderr=err)

     if (scenario == 'realize_annExperiment'):
          # comment out to keep current results and avoid accidental activation (unintended overwriting containers)
          #experiment01.realize_experiment()
          #experiment02.realize_experiment()
          #experiment03.realize_experiment()
          #experiment04.realize_experiment()
          #experiment05.realize_experiment()
          pass
     
     # for experiment05
     if (scenario == 'apply_annSolution_for_imageClassification') or (scenario == 'apply_annSolution_for_transportClassification') or (scenario == 'manual_sensorValueUpdate'):
          if (sub_process_method == "parallel"):
               # b) by subprocess.Popen()
               # Remark: By this variant, parallel requests at the same machine are realized in parallel. Hence, individual stdout and stderr have been created so that CLI output is separated correctly.
               # Please note, message broaker does not manage requests. Indeed, each machine requires a manager for efficient ressource allocation.
               with open(log_directory+"/"+sender+"_stdout.txt", "wb") as out, open(log_directory+"/"+sender+"_stderr.txt", "wb") as err:
                    # carry out current scenario
                    p = subprocess.Popen("docker-compose -f "+log_directory+"/"+sender+"-docker-compose.yml up --remove-orphans", shell=True, stdout=out, stderr=err)
                    print('Message of ' + sender + ' has been triggered at ' + receiver + ' successfully!')
                    # wait for finalization at experiment 5
                    p.wait(timeout=None)
                    if (scenario == 'apply_annSolution_for_imageClassification'):
                         # announce finalization of ANN requests
                         client.publish(MQTT_Topic_Results, 'This is a result indication! My name is '+ hostName+' and I have processed the ann request.')
                    if (scenario == 'apply_annSolution_for_transportClassification'):
                         # announce finalization of ANN requests
                         client.publish(MQTT_Topic_Results, 'This is a result indication! My name is '+ hostName+' and I have processed the ann request.')
                    if (scenario == 'manual_sensorValueUpdate'): # this can be used for testing
                         # announce finalization of sensor update
                         # maybe put this in a separate client, so that ANN requests can be realized at AI-Lab 
                         # and decentralized systems can be virtually realized at production systems...?
                         client.publish(MQTT_Topic_Results, 'This is a sonsor update indication! My name is '+ hostName+' and I have updated the files by given values.')

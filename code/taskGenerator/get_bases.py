# do this only once when starting the task scheduler so we get the file 
# to generate tasks to schedule with the task scheduler
# make sure you´re in the right directory to execute the code --> in AI-CPS-Masterarbeit-Lena

import os 

def get_bases():
    # Pfad zum Ordner der images
    image_folder_path = "./images"

    # Ordnerpfad, in dem die Datei gespeichert werden soll
    # output_folder = "./taskGenerator"

    # Dateiname mit vollständigem Pfad
    output_file = "./code/taskGenerator/bases.txt"

    # Alle Dateien und Ordner im Verzeichnis auflisten
    entries = os.listdir(image_folder_path)
    entries = ["marcusgrum/" + s.lower() for s in entries]

    # if you want to write the names into a file in case you want to use 
    # the code from a device where the image folder does not exist prepare to
    # read the bases one time
    # with open(output_file, "w") as file:
    #     for entry in entries:
    #         file.write(entry + "\n")  # Jeder Eintrag in eine neue Zeile

    # print(f"Filenames were written to {output_file}.")

    return entries

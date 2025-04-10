# Do this only once when starting the task scheduler so we get the file 
# To generate tasks to schedule with the task scheduler
# Make sure you´re in the right directory to execute the code --> in AI-CPS-Masterarbeit-Lena

import os 

def get_bases():
    # Path to "image" folder
    current_dir = os.path.dirname(os.path.abspath(__file__))
    image_folder_path = os.path.join(current_dir, "..", "..", "images")

    # Filename to write the names into
    output_file = "./code/taskGenerator/bases.txt"

    try:
        entries = os.listdir(image_folder_path)
        entries = ["marcusgrum/" + s.lower() for s in entries]
    except FileNotFoundError:
        print(f"Der Ordner '{image_folder_path}' konnte nicht gefunden werden.")
        return []
    
    # If you want to write the names into a file in case you want to use 
    # the code from a device where the image folder does not exist prepare to
    # read the bases one time
    # with open(output_file, "w") as file:
    #     for entry in entries:
    #         file.write(entry + "\n")

    # print(f"Filenames were written to {output_file}.")

    return entries

import platform
import os

# change power scheme in linux
def set_cpu_governor(governor="conservative"):
    """set CPU-Governor for DVFS"""
# performance – Maximale Leistung, keine Rücksicht auf Stromverbrauch
# powersave – Energiesparmodus, begrenzt die CPU-Frequenz
# ondemand – Passt die Frequenz dynamisch an die Last an -> das ist der Standard unter Linux
# conservative – Wie ondemand, aber langsamer und energiesparender
# userspace ist auch noch available
# schedutil – Nutzt den Linux-Scheduler zur Steuerung der Frequenz
# Wenn du energieeffizient arbeiten möchtest, hängt die Wahl zwischen conservative und powersave von deinem Anwendungsfall ab:
# 🔋 powersave
# ✅ Maximale Energieeinsparung
# ✅ Reduziert die CPU-Frequenz stark (meist auf das Minimum)
# ❌ Kann Leistung stark einschränken, langsameres Arbeiten möglich
# 📌 Ideal für: Maximale Akkulaufzeit, geringe Last (Surfen, Texte schreiben, Videos schauen)

# ⚖️ conservative
# ✅ Ähnlich wie ondemand, aber weniger aggressiv beim Hochskalieren
# ✅ Erhöht die Frequenz nur langsam und nur, wenn wirklich nötig
# ✅ Bessere Balance zwischen Stromsparen und Leistung als powersave
# 📌 Ideal für: Rechenaufgaben, die Energieeffizienz erfordern (leichte CPU-Last, Skripte, leichte Bildbearbeitung, Coding)
    try:
        for cpu in range(os.cpu_count()):
            governor_path = f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor"
            with open(governor_path, "w") as f:
                 f.write(governor)
            print(f"set CPU {cpu} to '{governor}'.")
    except PermissionError:
        print("Permissions denied. Please run file as root.")

# change power scheme in windows 
def set_power_scheme(scheme_guid):
    os.system(f"powercfg /setactive {scheme_guid}")

def get_device_info():
    system = platform.system()  # "Linux", "Windows", "Darwin" (Mac)
    machine = platform.machine()  # "x86_64", "armv7l", "aarch64" (Raspberry Pi)
    node = platform.node()  # Hostname (falls spezifisch benannt)

    if system == "Linux":
        if "arm" in machine or "aarch" in machine:
            return "Raspberry Pi"
        else:
            return "Linux"
    elif system == "Windows":
        return "Windows"
    elif system == "Darwin":
        return "Mac"
    else:
        return "Unknown"
            
# for a greener client lets activate power saving mode, but first check which os we use 
# and which architecture it is
def set_specific_power_scheme(scheme="balanced"):
    device_info = get_device_info()
    if device_info == "Windows":
        print("OS: Windows")
        if scheme == "balanced":
            set_power_scheme("381b4222-f694-41f0-9685-ff5bb260df2e")
            print("Set power scheme to balanced.")
        elif scheme == "save":
            set_power_scheme("a1841308-3541-4fab-bc81-f71556f20b4a")
            print("Set power scheme to saving.")
        elif scheme == "max":
            set_power_scheme("8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c")
            print("Set power scheme to max power.")
        else:
            print("please provide a right string e.g. balanced, save or max.")
    elif device_info == "Linux":
        print("OS: Linux")
        # set_cpu_governor()
        # print("Set power scheme to conservative.")
    elif device_info == "Darwin":
        print("OS: macOS")
    else:
        print(f"Unknow OS: {device_info}")
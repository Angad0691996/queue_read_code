import pyads
import json
import re
import time

AMS_NET_ID = '5.51.197.248.1.1'
ADS_PORT = 801

# Load queue node definitions (adjust filename if needed)
with open('nodes.txt', 'r') as f:
    nodes = json.load(f)

# Filter only queue status nodes (example: names containing "Request_Queue_Status")
queue_nodes = [n for n in nodes if "Request_Queue_Status" in n['name']]

# Map string types to pyads types
type_mapping = {
    "PLCTYPE_BOOL": pyads.PLCTYPE_BOOL,
    "PLCTYPE_INT": pyads.PLCTYPE_INT,
    "PLCTYPE_BYTE": pyads.PLCTYPE_BYTE,
    "PLCTYPE_UINT": pyads.PLCTYPE_UINT,
    "PLCTYPE_WORD": pyads.PLCTYPE_WORD
}

def clean_name(var_name):
    # Remove prefix and array brackets for readability
    name = var_name.replace(".PLC_To_Server.", "")
    name = re.sub(r"\[(\d+)\]", r"_\1", name)
    name = name.replace(".", "_")
    return name

def read_queue(plc):
    queue_data = []
    for node in queue_nodes:
        var_name = node['name']
        var_type = type_mapping.get(node['type'])
        try:
            value = plc.read_by_name(var_name, var_type)
            if value not in (0, None, 9999):  # Only non-zero, non-default
                queue_data.append({
                    "name": clean_name(var_name),
                    "value": value
                })
        except Exception as e:
            print(f"Error reading {var_name}: {e}")
    return queue_data

def main():
    plc = pyads.Connection(AMS_NET_ID, ADS_PORT)
    plc.open()
    print("Connected to PLC.")

    try:
        while True:
            queue_data = read_queue(plc)
            print("Non-zero queue entries:")
            for entry in queue_data:
                print(f"{entry['name']}: {entry['value']}")
            print("-" * 40)
            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        plc.close()

if __name__ == "__main__":
    main()
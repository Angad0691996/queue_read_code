#All three read included except for write_to_plc nodes and send_to_azure_iot_hub nodes
import pyads
from azure.iot.device import IoTHubDeviceClient, Message
import json
from datetime import datetime
import time
import threading
import signal
from threading import Lock
import queue
import re
 
 
custom_c = None
stop_thread = threading.Event()
last_processed_message_ids = set()
MAX_MESSAGE_ID_HISTORY = 1000
# Global queue to hold incoming requests
request_queue = queue.Queue()
client_lock1 = Lock()
client_lock2 = Lock()
 
 
 
def get_current_date_time():
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")
    return current_date, current_time
 
# TwinCAT ADS connection parameters
AMS_NET_ID = '5.51.197.248.1.1'  # Your AMS Net ID
ADS_PORT = 801  # Standard ADS port
PLC_IP = '192.168.1.1'
 
#CONNECTION_STRING= "HostName=WP-STAGING-2.azure-devices.net;DeviceId=WP-STAGING-Device03;SharedAccessKey=OqvWyxqL8QYKYVHgf39Jnto7rxWTZyLgki/+OJFkA2E="  
CONNECTION_STRING= "HostName=CarParking-T1.azure-devices.net;DeviceId=beckhoff-D1;SharedAccessKey=0nUlWgT2ySqehrqVvz1dOfknDiqRcfa3LAIoTHnohFQ="  
 
 
#Nodes to read from the PLC Parking site configuration and queue status updates
try:
    with open('nodes.txt', 'r') as file:
        PYADS_VARIABLES = json.load(file)
except Exception as e:
    print(" Eror loading pyads nodes: {e}")
    PYADS_VARIABLES = []
 
#Nodes to write to the PLC - Hardcoded
WRITE_NODES = [
    {"name": ".Server_To_PLC.Request_Data.Token_No", "type": "PLCTYPE_INT"},
    {"name": ".Server_To_PLC.Request_Data.Car_Type", "type": "PLCTYPE_BYTE"},
    {"name": ".Server_To_PLC.Request_Data.Request_Type", "type": "PLCTYPE_BYTE"},
    {"name": ".Server_To_PLC.Add_Request", "type": "PLCTYPE_BOOL"}    
]
 
def read_node_ids(file_path):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading node IDs from file: {e}")
        return {}
 
# Read node IDs from the file for parking_map
node_ids = read_node_ids("parking_maps.txt")
# Function to read non-zero values from specified nodes
 
def get_non_zero_values(client, node_ids):
    non_zero_values = []
    for key, node_id in node_ids.items():
        try:
            data_value = client.get_node(node_id).get_data_value()
            actual_value = data_value.Value
           
            # Check if the StatusCode is good
            if data_value.StatusCode.is_good():
                # Handle Variant values correctly
                if isinstance(actual_value, ua.Variant):
                    int_value = actual_value.Value
                    # Check for Int16 range
                    if isinstance(int_value, int) and -32768 <= int_value <= 32767:
                        if int_value > 0:
                            non_zero_values.append(int_value)
                else:
                    print(f"Unexpected type for {key}: {type(actual_value)}")
            else:
                print(f"Bad status for {key}: {data_value.StatusCode}")
        except Exception as e:
            print(f"Error reading {key}: {e}")
    return non_zero_values
 
 
def connect_to_plc():
    try:
        plc = pyads.Connection(AMS_NET_ID, ADS_PORT, PLC_IP)
        plc.open()
        print(f"Connected to PLC: {AMS_NET_ID}")
        return plc
    except Exception as e:
        print(f"Error connecting to PLC: {e}")
        return None
 
def read_plc_nodes(plc, plc_name):
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%H:%M:%S")
 
    data = {}
 
    type_mapping = {
        "PLCTYPE_BOOL": pyads.PLCTYPE_BOOL,
        "PLCTYPE_INT": pyads.PLCTYPE_INT,
        "PLCTYPE_BYTE": pyads.PLCTYPE_BYTE,
        "PLCTYPE_UINT": pyads.PLCTYPE_UINT,
        "PLCTYPE_WORD": pyads.PLCTYPE_WORD
    }
 
    # Read all values from PLC and store in `data`
    for var in PYADS_VARIABLES:
        var_name = var['name']
        var_type = type_mapping.get(var['type'])
        try:
            value = plc.read_by_name(var_name, var_type) if var_type else None
            # --- FIXED CLEAN NAME LOGIC ---
            clean_name = var_name.replace(".PLC_To_Server.", "")
            clean_name = re.sub(r"\[(\d+)\]", r"_\1", clean_name)
            clean_name = clean_name.replace(".", "_")
            data[clean_name] = value
            # print(f"Read {var_name} ({clean_name}): {value}")
        except Exception as e:
            print(f"Error reading {var_name}: {e}")
 
    # === PARKING_SITE_CONFIG ===
    formatted_dict1 = {
        "Message_Id": "PARKING_SITE_CONFIG",
        "System_Date": current_date,
        "System_Time": current_time,
        "System_Code_No": plc_name,
        "System_Type": data.get("System_Type"),
        "System_No": data.get("System_No"),
        "Max_Lift_No": data.get("Max_Lift_No"),
        "Max_Floor_No": data.get("Max_Floor_No"),
        "Max_Shuttle_No": data.get("Max_Shuttle_No"),
        "Total_Parking_Slots": data.get("Total_Parking_Slots"),
        "Slots_By_Type": [data.get(f"Type{i}_Slots") for i in range(1, 8)],
        "Total_Parked_Slots": data.get("Total_Parked_Slots"),
        "Parked_Slots_By_Type": [data.get(f"Type{i}_Parked_Slots") for i in range(1, 8)],
        "Total_Empty_Slots": data.get("Total_Empty_Slots"),
        "Empty_Slots_By_Type": [data.get(f"Type{i}_Empty_Slots") for i in range(1, 8)],
        "Total_Dead_Slots": data.get("Total_Dead_Slots"),
        "Dead_Slots_By_Type": [data.get(f"Type{i}_Dead_Slots") for i in range(1, 8)],
        "Total_Booked_Slots": data.get("Total_Booked_Slots"),
        "Booked_Slots_By_Type": [data.get(f"Type{i}_Booked_Slots") for i in range(1, 8)],
    }
 
    # === QUEUE_STATUS_UPDATES ===
    queue_data_list = []
 
    for i in range(1, 110):
        token_no = data.get(f"Request_Queue_Status_{i}_TokenNo")

        #print(f"DEBUG: Queue {i} TokenNo = {token_no}")
 
        if token_no in (None, 0, 9999):
            continue

        estimated_time = data.get(f"Request_Queue_Status_{i}_Estimated_Time")
        request_type = data.get(f"Request_Queue_Status_{i}_Request_Type")
        in_progress = data.get(f"Request_Queue_Status_{i}_Request_In_Progress")
 
 
        queue_data_list.append({
            "Token_No": token_no,
            "ETA": estimated_time,
            "Request_Type": request_type,
            "Request_In_Progress": in_progress
        })
 
    #print("DEBUG: Final Queue_Data:", queue_data_list)
 
    formatted_dict2 = {
        "Message_Id": "QUEUE_STATUS_UPDATES",
        "System_Date": current_date,
        "System_Time": current_time,
        "System_Code_No": plc_name,
        "System_Type": data.get("System_Type", "0"),
        "System_No": data.get("System_No", "0"),
        "Queue_Data": queue_data_list,
    }
   
    parking_map = create_parking_map_from_file(plc, plc_name, current_date, current_time, "parking_maps.txt", queue_data_list)
 
 
    return formatted_dict1, formatted_dict2, parking_map
    
def create_parking_map_from_file(plc, plc_name, current_date, current_time, parking_map_file, queue_data_list):
    try:
        with open(parking_map_file, 'r') as f:
            parking_map_nodes = json.load(f)

        # ✅ Read individual values instead of bulk read to avoid encoding issues
        non_zero_token_values = []
        for node_info in parking_map_nodes:
            var_name = node_info['name']
            data_type_str = node_info['type']
            data_type = getattr(pyads, data_type_str, None)
            if data_type:
                try:
                    value = plc.read_by_name(var_name, data_type)
                    if value is not None and value != 0:
                        non_zero_token_values.append(value)
                except Exception as read_err:
                    print(f"Error reading {var_name}: {read_err}")
            else:
                print(f"Warning: Unknown data type '{data_type_str}' for {var_name}, skipping.")

        # Note: non_zero_token_values already collected above, no additional filtering needed

        # ✅ New: Enhanced parking_map_resync logic (EXACT as you originally had)
        perform_parking_map_resync = 0
        try:
            read_parking_map_value = plc.read_by_name(".PLC_To_Server.Read_Parking_Map", pyads.PLCTYPE_BOOL)
            if read_parking_map_value:
                perform_parking_map_resync = 1

                try:
                    # Set acknowledgment to True
                    plc.write_by_name(".Server_To_PLC.Read_Parking_Map_Ack", True, pyads.PLCTYPE_BOOL)
                except Exception as ack_err:
                    print(f"Error writing Read_Parking_Map_Ack: {ack_err}")

                try:
                    # Reset acknowledgment back to False
                    plc.write_by_name(".Server_To_PLC.Read_Parking_Map_Ack", False, pyads.PLCTYPE_BOOL)
                except Exception as reset_ack_err:
                    print(f"Error resetting Read_Parking_Map_Ack: {reset_ack_err}")
        except Exception as resync_err:
            print(f"Error handling resync logic: {resync_err}")

        # ✅ Final parking map structure
        parking_map = {
            "Message_Id": "PARKING_MAP",
            "System_Date": current_date,
            "System_Time": current_time,
            "System_Code_No": plc_name,
            "System_Type": "0",
            "System_No": "0",
            "Is_PLC_Connected": "1",
            "Perform_Parking_Map_Resync": perform_parking_map_resync,
            "Token_No": non_zero_token_values,
            "Queue_Data": queue_data_list
        }

        return parking_map

    except FileNotFoundError:
        print(f"Error: {parking_map_file} not found.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {parking_map_file}.")
        return {}
    except Exception as e:
        print(f"Unexpected error creating parking map: {e}")
        return {}



    
def read_request_type(plc):  # server to plc
    try:
        request_type_value = plc.read_by_name(".Server_To_PLC.Request_Data.Request_Type", pyads.PLCTYPE_BYTE)

       
        # Map the request type values
        if request_type_value == 1:
            return 3
        elif request_type_value == 4:
            return 6
        elif request_type_value == 2:
            return 2
        elif request_type_value == 3:
            return 5
       
        # If no mapping was applied, return the original value
        return request_type_value

    except Exception as e:
        print(f"Error reading Request_type value: {e}")
        return None
 
 
def write_to_plc(plc, data, type_mapping):
    """
    Writes IoT message values to Beckhoff PLC nodes and handles acknowledgment logic.
    """
    try:
        print("🚀 Starting Beckhoff write operation")
 
        # Data mapping for clean key matching
        data_key_mapping = {
            "Token_No": "Token_No",
            "Car_Type": "Car_Type_Value",
            "Request_Type": "Request_Type_Value",
            "Add_Request": "Add_Request"
        }
 
        # Add this inside your write_to_plc function before writing Request_Type_Value
        request_type_map = {3: 1, 2: 2, 6: 4, 5: 3}
        if "Request_Type_Value" in data:
            data["Request_Type_Value"] = request_type_map.get(data["Request_Type_Value"], data["Request_Type_Value"])
 
        # Write each node
        for node in WRITE_NODES:
            var_name = node['name']
            var_type = type_mapping.get(node['type'], None)
            key_name = var_name.split('.')[-1]
            mapped_key = data_key_mapping.get(key_name)
 
            if mapped_key and mapped_key in data:
                try:
                    value = data[mapped_key]
                    plc.write_by_name(var_name, value, var_type)
                    print(f"✅ Written {value} to {var_name}")
                except Exception as e:
                    print(f"❌ Error writing {var_name}: {e}")
            else:
                print(f"⚠️ Skipped {var_name} (Not in data)")
 
        # ✅ Toggle Add_Request to True
        plc.write_by_name(".Server_To_PLC.Add_Request", True, pyads.PLCTYPE_BOOL)
        print("✅ Written True to .Server_To_PLC.Add_Request")
 
        time.sleep(0.2)  # Allow PLC to process the request
 
        # 🟡 Read Request_Ack
        request_ack = plc.read_by_name(".PLC_To_Server.Request_Ack", pyads.PLCTYPE_BYTE)
        print(f"📥 Request_Ack read from PLC: {request_ack}")
 
        # 🟡 Read Token_No
        token_no = plc.read_by_name(".Server_To_PLC.Request_Data.Token_No", pyads.PLCTYPE_INT)  # Adjust index if needed
        print(f"📥 Token_No read from PLC: {token_no}")
 
        # 🟡 Read System_Code_No
        system_code_no = plc.read_by_name(".PLC_To_Server.System_Code_No", pyads.PLCTYPE_WORD)  # Adjust index if needed
        #print(f"📥 System_Code_No read from PLC: {system_code_no}")
       
        # 🟡 Read Requests_type_Value
        #request_type_value = plc.read_by_name(".Server_To_PLC.Request_Data.Request_Type", pyads.PLCTYPE_BYTE)  # Adjust index if needed
        #print(f"📥 Request_Type read from PLC: {request_type_value}")
        request_type_value = read_request_type(plc)
        

        # ✅ Reset Add_Request
        if request_ack is not None and request_ack > 0:
            plc.write_by_name(".Server_To_PLC.Add_Request", False, pyads.PLCTYPE_BOOL)
            print("✅ Written False to .Server_To_PLC.Add_Request")
 
            # 🌐 Prepare and send acknowledgment
            current_date, current_time = get_current_date_time()
            ack_message = {
                "Message_Id": "REQUEST_ACKNOWLEDGEMENT",
                "System_Date": current_date,
                "System_Time": current_time,
                "System_Code_No": system_code_no,
                "System_Type": "0",
                "System_No": "0",
                "Token_No": token_no,
                "Request_Type_Value": request_type_value,
                "Ack_Status": request_ack
            }
 
            send_to_azure_iot_hub(ack_message, custom_c)
            print("📤 Sent acknowledgment to Azure IoT Hub")
            time.sleep(0.05)  # Allow time for PLC to process the acknowledgment
 
        else:
            print("⚠️ Request_Ack not ready, skipping acknowledgment")
 
    except Exception as e:
        print(f"❌ General write error: {e}")
 
 
 
# Send data to Azure IoT Hub
def send_to_azure_iot_hub(json_output, client):
    try:
        if custom_c is None:
            return
 
        json_str = json.dumps(json_output)
        message = Message(json_str)
        custom_c.send_message(message)
        print(f"Message sent to Azure IoT Hub: {json_output}")
 
    except Exception as e:
        print(f"Error sending message to Azure IoT Hub: {e}")
 
# Process request queue for writing to PLC
def process_queue(plc):
    # Define type mapping here as well
    type_mapping = {
        "PLCTYPE_INT": pyads.PLCTYPE_INT,
        "PLCTYPE_BYTE": pyads.PLCTYPE_BYTE,
        "PLCTYPE_BOOL": pyads.PLCTYPE_BOOL
    }

    while True:
        data = request_queue.get()
        if data is None:
            break  # Exit signal
        # Pass type_mapping as the third argument
        time.sleep(0.1)
        with client_lock1:  # Use lock to prevent concurrent access
            write_to_plc(plc, data, type_mapping)
        request_queue.task_done()
 
# Continuously read from PLC and send data to Azure
def send_data_continuously(interval, plc):
    while not stop_thread.is_set():
        start = time.time()
        plc_name = "PRT79"

        try:
            with client_lock2:  # Use different lock for reading operations
                # --- Heartbeat logic ---
                maintenance_mode_value = plc.read_by_name(".PLC_To_Server.Maintenance_Mode", pyads.PLCTYPE_BOOL)

                hbr_value = plc.read_by_name(".PLC_To_Server.Heartbeat", pyads.PLCTYPE_BYTE)
                plc.write_by_name(".Server_To_PLC.Heartbeat", hbr_value, pyads.PLCTYPE_BYTE)
                time.sleep(0.01)
                hbr_new_value = plc.read_by_name(".PLC_To_Server.Heartbeat", pyads.PLCTYPE_BYTE)

                # Determine PLC connection status
                is_plc_connected = 1 if hbr_new_value != hbr_value and maintenance_mode_value == 0 else 0

                # --- Heartbeat message ---
                t0 = time.time()
                current_date = time.strftime("%Y-%m-%d")
                current_time = time.strftime("%H:%M:%S")

                heartbeat_message = {
                    "Message_Id": "PLC_HEARTBEAT",
                    "System_Date": current_date,
                    "System_Time": current_time,
                    "System_Code_No": plc_name,
                    "System_Type": 0,
                    "System_No": 0,
                    "Is_PLC_Connected": is_plc_connected,
                }

                send_to_azure_iot_hub(heartbeat_message, custom_c)
                print(f"⏱ Heartbeat took: {time.time() - t0:.2f} seconds")

                # Toggle new heartbeat value back
                plc.write_by_name(".Server_To_PLC.Heartbeat", hbr_new_value, pyads.PLCTYPE_BYTE)
                time.sleep(0.05)

                if not is_plc_connected:
                    print(f"⚠️ PLC '{plc_name}' not connected. Skipping data read.")
                    continue

                # --- Data sending logic ---
                t1 = time.time()
                data1, data2, parking_map = read_plc_nodes(plc, plc_name)
                print(f"⏱ PLC read took: {time.time() - t1:.2f} seconds")

                t2 = time.time()
                for dataset in (data1, data2, parking_map):
                    if dataset:
                        send_to_azure_iot_hub(dataset, custom_c)
                print(f"⏱ Azure send took: {time.time() - t2:.2f} seconds")

        except Exception as e:
            print(f"❌ Error in PLC '{plc_name}' loop: {e}")

        # --- Cycle timing ---
        elapsed = time.time() - start
        print(f"⏱ Total cycle time: {elapsed:.2f} seconds")

        if elapsed < interval:
            time.sleep(interval - elapsed)


 
def on_message_received(message):
    try:
        raw_data = message.data.decode('utf-8').strip()
        print(f"📩 Received message from Azure IoT Hub: {raw_data}")
 
        data = json.loads(raw_data)  # Convert message to Python dict
        print(f"Actual received data: {data}")

        # Add the request to the queue with hardcoded Add_Request set to True
        non_plc_data = {
            "Token_No": data.get("Token_No"),
            "Car_Type_Value": data.get("Car_Type_Value"),
            "Request_Type_Value": data.get("Request_Type_Value"),
            "Add_Request": True
        }
 
        # Ensure no None values in the data dictionary
        if all(value is not None for value in non_plc_data.values()):
            print(f"📌 Adding to request queue: {non_plc_data}")
            request_queue.put(non_plc_data)
        else:
            print(f"⚠️ Skipped adding to queue due to missing data: {non_plc_data}")
 
    except Exception as e:
        print(f"❌ Error processing Azure message: {e}")
 
 
 
 
# Main execution
if __name__ == "__main__":
    c = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)
    custom_c = c
 
    signal.signal(signal.SIGINT, lambda sig, frame: stop_thread.set())
    signal.signal(signal.SIGTERM, lambda sig, frame: stop_thread.set())
 
    # Connect to PLC
    plc = connect_to_plc()
    if not plc:
        exit()
 
    # Start worker thread for processing PLC requests
    worker_thread = threading.Thread(target=process_queue, args=(plc,))
    worker_thread.start()
 
    # Start the thread for sending data continuously
    send_data_thread = threading.Thread(target=send_data_continuously, args=(5, plc))  # or 10
    send_data_thread.start()
 
    # Start listening to Azure IoT Hub
    c.on_message_received = on_message_received
    c.connect()
   
    send_data_thread.join()
    request_queue.put(None)  # Signal queue to exit
    worker_thread.join()
 
    print("Program exited cleanly.")

import requests
import json
import paho.mqtt.client as mqtt
import time
from datetime import date

# Variables
ec2_address = "18.209.179.7"
smart_contract_raw_endpoint = "https://u0ic3e50kv-u0me9cmixf-connect.us0-aws.kaleido.io/instances/itemmanagementv2"
topics = [("object_recognition", 2), ("environment/humidity", 2), ("environment/temperature", 2)]

# Generate API endpoint for corresponding action
# getItem, getItemCount
def url_generation(action):
    url = smart_contract_raw_endpoint
    param = "?kld-from=0xa38f7ce1f353a8B11c5bd1B2da6BE7EfF405b3c9"
    if action.lower() == "addItem".lower():
        url = url + "/addItem" + param
    
    elif action.lower() == "getItemCount".lower():
        url = url + "/getItemCount" + param
    
    elif action.lower() == "addHumidityViolation".lower():
        url = url + "/addHumidityViolation" + param
    
    elif action.lower() == "addTempViolation".lower():
        url = url + "/addTempViolation" + param
    
    return url

# Generate headers
# Include Authorization (Basic Authorization) and Content-Type
def headers_generation():
    headers = {
        'Authorization': 'Basic dTBuNzR5Y3lhOTpYYm8zbWFvdzhzN0hZcGh3ekE3UDdteVYxUXRmdVFtTGFkck1IMDNMX0E4',
        'Content-Type': 'application/json'
    }

    return headers

# Generate bodies
# Need asset_type, date, manufacturer, serial_number, and time
def bodies_generation(action, *args):

    if action.lower() == "addItem".lower():
        print("here1")
        bodies = {
            "my_asset_type": args[0],
            "my_manufacturer": args[1],
            "my_th_humidity": args[2],
            "my_th_temp": args[3],
            "my_time": args[4],
            "my_serial_number": args[5]
        }
        print("This is bodies in message_generation: ", bodies)
    
    elif action.lower() == "addHumidityViolation".lower() or action.lower() == "addTempViolation".lower():
        print("here2")
        bodies = {
            "my_serial_number": args[0],
            "my_value": args[1],
            "my_time": args[2]
        }
    
    return bodies


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connection Made Successfully, Connecting to: ", client)
    else:
        print("Connection Failed")

def on_disconnect(client, userdata, flags, rc=0):
    print("Disconnected. Result Code: ", rc)

def on_message(client, userdata, message):
    print("message from: ", message.topic, " received")
    temp = str(message.payload)[2:len(str(message.payload))-1]
    fields = temp.split(",")

    # print("A")
    if message.topic == topics[0][0]:
        # print("A-11")
        url = url_generation("addItem")
        print("This is url:", url)
        # print("A-12")
        bodies = json.dumps(bodies_generation("addItem", str(fields[0]), str(fields[1]), str(fields[2]), str(fields[3]), str(fields[4]), str(fields[5])))
        print("This is bodies", bodies)
        print("A-13")
    elif message.topic == topics[1][0]:
        # print("A-21")
        url = url_generation("addHumidityViolation")
        print("This is url:", url)
        # print("A-22")
        bodies = json.dumps(bodies_generation("addHumidityViolation", str(fields[0]), str(fields[1]), str(fields[2])))
        print("This is bodies", bodies)
        print("A-23")
    elif message.topic == topics[2][0]:
        # print("A-31")
        url = url_generation("addTempViolation")
        print("This is url:", url)
        # print("A-32")
        bodies = json.dumps(bodies_generation("addTempViolation", str(fields[0]), str(fields[1]), str(fields[2])))
        print("This is bodies", bodies)
        print("A-33")
    # print("B")
    headers = headers_generation()
    # print("C")
    response = requests.request("POST", url, headers=headers, data=bodies)
    print(response.text.encode("utf8"))
    print("============================================================")


broker = ec2_address
client = mqtt.Client("ec2_mqtt_client")

client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

print("Connecting to broker: ", broker)

client.connect(broker)

# infinite loop to maintain subsription process
while True:
    client.loop_start()
    client.subscribe(topics)
    client.loop_stop()

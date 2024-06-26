import os
import configparser
import requests
import paho.mqtt.client as mqtt
import logging
import time
from datetime import datetime

# Add and setup logging to a file in /root/mqtt which is also where  I  store the  script
logging.basicConfig(filename='/root/mqtt/mqtt.log',filemode='a',format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',datefmt='%H:%M:%S',level=logging.INFO)

logging.info("Running MQTT Publisher")

logger = logging.getLogger('MQTT')
# Other definitions including your broker address
han_host = os.getenv('HAN_API_HOST')
# Read in broker config  from mqtt.cfg
config = configparser.ConfigParser()
config.read("mqtt.cfg")

if config.read("/root/mqtt/mqtt.cfg"):
	logger.info("Reading from config file "+str(config.read("/root/mqtt/mqtt.cfg")))
	broker=config.get("broker","ip")
	port=config.get("broker","port")
	logger.info("Using "+broker+" as the broker")
	logger.info("And "+str(port)+" as the port")
	# check for authentication section if it exists use authentication
	authentication=config.has_section("authentication")
	if authentication:
		logger.info("Using authentication as defined in mqtt.cfg")
		username=config.get("authentication","username")
		password=config.get("authentication","password")
else:
	logger.error("Config file mqtt.cfg not found, using defaults")	
	broker="192.168.1.41"
	port=1883
	authentication=False
# Create a flag in class to track MQTT broker connection success
mqtt.Client.connected_flag=False

def on_publish(client, userdata, mid, reason_codes, properties):             #create function for callback
	logger.info("MQTT topic published")
	pass

def on_connect(client, userdata, flags, reason_code, properties):
	#if flags.session_present:
        	# ...
	if reason_code == 0:
		# successful connection
		logger.info("Connected to MQTT Broker  Returned code="+str(reason_code))
		client.connected_flag = True

	if reason_code > 0:
        # error processing
		logger.error("Bad connection to MQTT BrokerReturned code="+str(reason_code))

def on_disconnect(client, userdata, flags, reason_code, properties):
	logger.info("Disconnected from MQTT broker  "+str(reason_code))
	client.connected_flag = False	

#   start the client and define callbacks
client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_publish = on_publish
if authentication:

	client.username_pw_set(username, password)
	# For testing the password will be writtent to mqtt.log, comment out the line below to stop this
	logger.info("Using username: "+username+" and password of "+password+" with authentication")

logger.info("Connecting to Broker "+broker)
# Run the main loop
client.loop_start()
try:
	client.connect(broker, int(port), 60)
except:
	logger.info("Connection to "+broker+" failed")
while not client.connected_flag: #wait in loop
	logger.warning("Waiting for Broker connection")
	time.sleep(1)
logger.info("Started Main Loop")
while True:
	#Check broker connection and wait for reconnect
	if client.connected_flag:
		pass
	else:
		while not client.connected_flag: #wait in loop
			logger.info("Waiting for Broker connection")
			time.sleep(5)
	# set timestamp for this update loop
	current_timestamp = str(datetime.utcnow())
	logger.info("Update cycle started "+current_timestamp)
	# Get elec meter consumption
	try:
		consump_response = requests.post(han_host + "/get_meter_consumption", json={"meter_type": "elec"})
		if consump_response.ok:
			elec_meter_consumption = consump_response.json()["meter_consump"]
			logger.info("Electricity Consumption returned")
			#print("Meter consumption for {} meter: {}".format("elec", elec_meter_consumption))
		else:
			logger.error("Error calling elec get_meter_consumption API: {}".format(consump_response.json()["Status"]))
	except:
		logger.error("Error in elec  get_meter_consumption API try block")
	# Get elec meter status
	try:
		status_response = requests.post(han_host + "/get_meter_status", json={"meter_type": "elec"})
		if status_response.ok:
			elec_meter_status = status_response.json()["meter_status"]
			logger.info("Electricity Meter Status returned")
			#print("Meter status for {} meter: {}".format("elec", elec_meter_status))
		else:
			logger.error("Error calling get_meter_status API: {}".format(elec_meter_status_response.json()["Status"]))
	except:
			logger.error("Error in elec  get_meter_consumption API try block")
	# Get gas  meter consumption
	try:
		gas_consump_response = requests.post(han_host + "/get_meter_consumption", json={"meter_type": "gas"})
		if gas_consump_response.ok:
			gas_meter_consumption = gas_consump_response.json()["meter_consump"]
			logger.info("Gas Consumption returned")
			#print("Meter consumption for {} meter: {}".format("gas", gas_meter_consumption))
		else:
			logger.error("Error calling get_meter_consumption API: {}".format(gas_consump_response.json()["Status"]))
	except:
			logger.error("Error in Gas  get_meter_consumption API try block")
	# Get gas meter status
	try:
		gas_status_response = requests.post(han_host + "/get_meter_status", json={"meter_type": "gas"})
		if gas_status_response.ok:
			gas_meter_status = gas_status_response.json()["meter_status"]
			logger.info("Gas Meter Status returned")
			#print("Meter status for {} meter: {}".format("gas", gas_meter_status))
		else:
			logger.error("Error calling get_meter_status API: {}".format(gas_meter_status_response.json()["Status"]))
	except:
			logger.error("Error in gas  get_meter_status API try block")
	#Publish to  MQTT Broker
	try:
		ret= client.publish("homepro/elec_meter",elec_meter_consumption)
		ret2 = client.publish("homepro/elect_meter_status", elec_meter_status)
		ret3 = client.publish("homepro/gas_meter", gas_meter_consumption)
		ret4 = client.publish("homepro/gas_meter_status", gas_meter_status)
	except:
		logger.error("Error in publishing MQTT data")
	time.sleep(5)

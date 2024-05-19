# homepro_mqtt
MQTT script for the Home Pro provided on an entirely "as is" basis, use entirely at your own risk.  More than happy for others to contribute, and I am learning as I go.

Basic Instructions

###SSH into your home pro

`ssh -p 8023 -o HostKeyAlgorithms=+ssh-rsa root@<home_pro_ip>`

###Install paho

`pip3 install paho-mqtt`

### Create the mqtt directory

`
mkdir mqtt
cd mqtt
nano mqtt_publisher.py
`
Create the Script

Paste in the code from the repository (edit in the correct address for your broker)

Test it by running 

`python3 mqtt_publisher.py`

Use your preferred tool to check if the topics are posted to your broker

check out `mqtt.log` in the mqtt folder to see if any errors or posted or if it is publishing messages succesfully.

### Make it Start automagically (but with the risk you may need a full reset and wipe your container if something goes wrong !!)
### Proceed only if you accept the risk of becoming even more familiar with the hard reset process !!

Create a shell script in the root directory to act as a wrapper in startup.sh

`nano start_mqtt.sh`

Insert the following
```
#!/bin/bash
/usr/bin/python3 /root/mqtt/mqtt_publisher.py &
```
make the script executable

`chmod +x start_mqtt.sh`

test it by running it

`./start_mqtt.sh`

check if your broker is recieving messages in your favourite tool (I use MQTT explorer), and look for any errors in mqtt.log

### If all is good, edit your startup.sh so it looks like this

```
#!/bin/bash

# Ensure our main env shows up in ssh sessions
# we're passing on API host info
env | grep _ >> /etc/environment
# Start mqtt_publisher
/root/start_mqtt.sh
# Start the ssh server
/usr/sbin/sshd -D
```
* Finally power cycle your home pro (entirely at your own risk, the only recovery at present if this fails is a hard reset, and your container will be wiped back to default)*

This is very experimental and has little error checking or automatic recovery if it dies for whatever reason it will need to be restarted manually or a power cycle.

I will look if it seems necessary to build a watchdog capability to restart if the process is not detected, but will test for now.

At present the script still publishes updates even the home pro is in the dreaded flashing CAD state, I may enhance this to check and skip publishing in that circumstance, and alternate thought will be this will show the duration of any meter dropouts so both approaches may be beneficial depending on what you do with the data afterwards.



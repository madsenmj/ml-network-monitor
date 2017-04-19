# RaspberryPi IoT Simulator

These python programs test and simulate an IoT device that is recording network events from the Measurement Lab NDT data archive.

# Requirements

- [The Paho MQTT client](https://pypi.python.org/pypi/paho-mqtt)

- The Maxmind GeoLite2 [Free Downloadable City Database](http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip), unzipped as a CSV and put in the same folder as the python files.

# Testing Mosquitto

To test the Mosquitto system:
In separate terminal windows do the following:
1. Start the broker: `mosquitto`
2. Start the command line subscriber:	`mosquitto_sub -v -t 'test/topic'`
3. Publish test message with the command line publisher: `mosquitto_pub -t 'test/topic' -m 'helloWorld'`

As well as seeing both the subscriber and publisher connection messages in the broker terminal the following should be printed in the subscriber terminal: `test/topic helloWorld`

# Python Mosquitto Code

I wrote a simple [python program](mqtt_test.py) for testing the Mosquitto system. Change the serverIP to match the IP address of the mqtt listening server. 

# Python NDT Test Data Generator

I have two programs to generate NDT testing data (same schema as the final project, but using random values), both found in the [RaspberryPi](/src/RaspberryPi) folder.
1. The first generates a list of sample values as if they came from the NDT data: `test_data_generator.py`
2. The second tests the MQTT transmission of data values to the IoT server: `test_data_stream.py`




# Test Code

1) Get the Raspberry PI turned on and running
2) Open /home/pi/IoT/parseMLabData.py
3) Update the dataday on line 20 to the day you want to scan
4) Get the IP address from the HDP VM (ifconfig on the command line)
5) Make sure the parseMLabData.py file is pointed to the correct IP on line 60
6) Start the mosquitto service on the PI: "sudo service mosquitto start"
7) Run the python file – it will pull down data from the Google network, then prompt for an "Enter" press when it is ready to start broadcasting. 
8) Start the mosquitto service on the HDP VM: "service mosquitto start"
9) Start the Storm Cluster: in the /root/Allegient directory run: "storm jar target/allegient-storm-1.0.jar com.allegient.storm.MQTTHivePBITopology"
10) Wait for the storm topology to load and provide a prompt saying it is looking for the MQTT spout.
11) Start the MQTT stream by pressing "Enter" on the Raspberry PI
12) Monitor the output of the data stream on Power BI: The DataNDT dashboard is live.


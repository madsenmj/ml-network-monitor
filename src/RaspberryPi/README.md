# RaspberryPi IoT Simulator

These python programs test and simulate an IoT device that is recording network events from the Measurement Lab NDT data archive.

# Requirements

- [The Paho MQTT client](https://pypi.python.org/pypi/paho-mqtt)
- Download and install the moquitto broker: https://mosquitto.org/download/
- Download and install GSUtils: https://cloud.google.com/storage/docs/gsutil_install
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

# Python IoT Simulator

The final file, `parseMLabData.py`, is the IoT simulator file. It does the following:
1. Get the list of NDT files available on the Google store using the gsutil command line function
2. Retrieve all of the files that belong to the server of interest
3. Unzip and parse the files, extracting the NDT data
4. Format the data for transmission via MQTT
5. Send events, 1 per second, to the MQTT serverIP





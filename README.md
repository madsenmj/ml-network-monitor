# IoT Real-time monitoring Demo

Using [NDT](https://www.measurementlab.net/tools/ndt/) data as the basis for doing an IoT monitoring Demo.

Presentation and YouTube video.

## Gathering NDT Data from Google BigQuery

The measurementlab data are stored in the public Google BigQuery database using [this schema](https://www.measurementlab.net/data/bq/schema/). I am interested in the Network Diagnostics Test (NDT) data. In particular, I am interested in the [following features](https://github.com/ndt-project/ndt/wiki/NDTDataFormat):

- `web100_log_entry.log_time`: The date and time of the log entry as `day`
- `web100_log_entry.connection_spec.local_ip`: The ip address of the NDT server as `severip`
- `web100_log_entry.connection_spec.remote_ip`: The ip address of the client that called the NDT server as `clientip`
- `web100_log_entry.connection_spec.remote_port`: The port from which the client called the NDT server as `clientport`
- `connection_spec.client_geolocation.latitude`: A derived value of the client's geographic latitude based on an IP lookup as `clientlat`
- `connection_spec.client_geolocation.longitude`: A derived value of the client's geographic longitude based on an IP lookup as `clientlon`
- `web100_log_entry.snap.CountRTT`:  The number of round-trip samples tested. I only use events where the count > 10. This effectively ignores queries that didn't go through properly as `countrtt`
- `web100_log_entry.snap.SumRTT`: The total time for all the round trip transmissions made by the NDT as `sumrtt`

There are two more elements that are extracted from this data:
- `sumrtt/countrtt`: The average round trip time as `avgrtt`
- `clientsub`: The client subnet, extracted from the client IP address as `clientsub`



### Preliminary Data Exploration


# Data Schema

## Data from IoT NDT Simulator

NDT data from the RapsberryPi IoT simulator is passed to the IoT server using the MQTT protocol. Events are passed with the following comma-separated schema:

day,serverip,clientip,clientport,clientlat,clientlon,countrtt,sumrtt,avgrtt,clientsub



## Importing Data to Hadoop/Hive

### Hortonworks Sandbox



## RaspberryPi Simulated Device



## Apache Storm IoT Hub


## Realtime Monitoring in PowerBI


# Running the Demonstration

1. Get the Raspberry PI turned on and running
2. Edit parseMLabData.py
3. Update the dataday on line to the day you want to scan
4. Get the IP address from the MQTT server 
5. Make sure the parseMLabData.py file is pointed to the correct IP
6. Start the mosquitto service on the PI: "sudo service mosquitto start"
7. Run the python file – it will pull down data from the Google network, then prompt for an "Enter" press when it is ready to start broadcasting. 
8. Start the mosquitto service on the server: "service mosquitto start"
9. Start the Storm Cluster: "storm jar target/storm-1.0.jar storm.MQTTHivePBITopology"
10. Wait for the storm topology to load and provide a prompt saying it is looking for the MQTT spout.
11. Start the MQTT stream by pressing "Enter" on the Raspberry PI
12. Monitor the output of the data stream on Power BI: The DataNDT dashboard is live.
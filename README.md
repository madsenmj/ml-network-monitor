# IoT Real-time monitoring Demo

Using [NDT](https://www.measurementlab.net/tools/ndt/) data as the basis for doing an IoT monitoring Demo.

I created a [PowerPoint presentation](/docs/Network_Monitor_Demo.pptx) and [YouTube video] (https://youtu.be/V0ljtJ_Y9Bk).

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

## Data collection
I pulled the historical NDT data in chunks of 16000 rows because the csv export is limited by their free interface to 16000 rows in one file. The query is:
 
```
SELECT
  STRFTIME_UTC_USEC(web100_log_entry.log_time * INTEGER(POW(10, 6)),
                    '%Y-%m-%dT%H:%M:%S') AS datetime,
  web100_log_entry.connection_spec.local_ip as serverip,
  web100_log_entry.connection_spec.remote_ip as clientip,
  web100_log_entry.connection_spec.remote_port as clientport,
  connection_spec.client_geolocation.latitude as client.lat,
  connection_spec.client_geolocation.longitude as client.lon,
  web100_log_entry.snap.CountRTT as countrtt,
  web100_log_entry.snap.SumRTT as sumrtt,
  web100_log_entry.snap.SumRTT/web100_log_entry.snap.CountRTT as avgrtt,
  FORMAT_IP(PARSE_IP(web100_log_entry.connection_spec.remote_ip)
        & INTEGER(POW(2, 32) - POW(2, 32 - 24))) as clientsub  
FROM
  plx.google:m_lab.ndt.all
WHERE
  web100_log_entry.connection_spec.remote_ip IS NOT NULL AND web100_log_entry.snap.CountRTT > 10 AND
  web100_log_entry.connection_spec.local_ip=='4.71.210.211' AND 
        STRFTIME_UTC_USEC(web100_log_entry.log_time * INTEGER(POW(10, 6)),
                    '%Y-%m-%dT%H:%M:%S') > '2015-01-01T00:00:00'
ORDER BY
  datetime ASC
LIMIT 16000;
```
 
I ran the queries one at a time, noting the final time in each block of 16000 rows and updating the `WHERE` clause accordingly to start collecting entries after the last time. Doing this, I pulled data from 2015-01-01T00:00:00 to 2016-05-03T20:08:50 as the historical training data from the MLab servers. Each query was saved in a folder using sequential file names. I then combined all the data into a single file using a `cat` command: `cat * > combined.csv`.

## Preliminary Data Exploration

Initial data exploration and processing was done using `R`. The data exploration notebooks is [in the R directory] (/src/R/NDT_data_exploration.ipynb).

## Data Schema

## Data from IoT NDT Simulator

NDT data from the RapsberryPi IoT simulator is passed to the IoT server using the MQTT protocol. Events are passed with the following comma-separated schema:

``` 
	day (string),
	serverip (string),
	clientip (string),
	clientport (integer),
	clientlat (float),
	clientlon (float),
	countrtt (integer),
	sumrtt (integer),
	avgrtt (float),
	clientsub (string)
```


# Importing Data to Hadoop/Hive

The Hadoop/Hive schema is as follows:

```
daytime STRING,  
serverip STRING,  
clientip STRING,  
clientport INT,  
clientLat FLOAT,  
clientLon FLOAT,  
countrtt INT,  
sumrtt INT,  
avgrtt FLOAT,  
ninsub INT,  
avginsub FLOAT,  
sigmainsub FLOAT, 
datafreq FLOAT, 
outlier INT,  
dataday STRING
```


### Hortonworks Sandbox

I configured a [Hortonworks Data Platform (HDP) sandbox](https://hortonworks.com/downloads/#sandbox). I uploaded the NDT data and stored it in the /user/root/NDT folder. I then ran the [hive script](/src/hiveimport.hive) to create the data table in hive and import the data from the csv into the table.

I also installed the Ecllipse IDE on the HDP in order to configure the Apache storm topology.


## RaspberryPi Simulated Device

The configuration, programs, and testing for the RapsberryPi IoT simulator are located [here](/src/RapsberryPi).

## Apache Storm IoT Hub

The configuration for the Apache Storm IoT hub is located [here](/src/Java).

## Realtime Monitoring in PowerBI

The data from the Storm topology was pushed into PowerBI. 

The data schema is:

```

AvgRTT (float)
ClientIP (string)
ClientLat (float)
ClientLon (float)
ClientSub (string)
Date(string) 
Outlier (bool)
QueryFreq (float)

```

There were a few things needed to get the PowerBI dashboard working using a bash script.

To talk to Power BI I first got the `OAUTH_CLIENT_ID` and `OAUTH_CLIENT_SECRET` from the PowerBI online portal.

``` 
#!/bin/bash
OAUTH_CLIENT_ID="<client id>"
OAUTH_CLIENT_SECRET="<client secret>"
```

I also set the username and password for the PowerBI dashboard.

```
OAUTH_USERNAME="username@company.com"
OAUTH_PASSWORD="<userpassword>"
```

I tested posting to the dashboard:

``` 
POST_RESULT="$(curl -s -X POST -d "resource=https://analysis.windows.net/powerbi/api&client_id="$OAUTH_CLIENT_ID"&client_secret="$OAUTH_CLIENT_SECRET"&grant_type=password&username="$OAUTH_USERNAME"&password="$OAUTH_PASSWORD"&scope=openid" "https://login.windows.net/<loginGroup>.onmicrosoft.com/oauth2/token" | jq -r .)"

 
echo ${POST_RESULT}
 
REFRESH_TOKEN="$(echo ${POST_RESULT} | /usr/bin/jq -r .refresh_token)"
ACCESS_TOKEN="$(echo ${POST_RESULT} | /usr/bin/jq -r .access_token)"
AUTH_HEADER="Authorization: Bearer ${ACCESS_TOKEN}"
echo "${AUTH_HEADER}"
 
echo "${AUTH_HEADER}" > ./auth_header.txt
echo "${REFRESH_TOKEN}" > ./refresh_token.txt

```
 
Then this:

```
#!/bin/bash
 
AUTH_HEADER=$(<./auth_header.txt)
curl -k -s "https://api.powerbi.com/beta/myorg/datasets" -H "$AUTH_HEADER" | /usr/bin/jq -r .
```

The final query returned the datasets from PowerBI. This all got coded into the PowerBI bolt in the Storm toplology.

Finally, to get the live refresh, the report in PowerBI has to be pinned to the dashboard and viewed there.

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
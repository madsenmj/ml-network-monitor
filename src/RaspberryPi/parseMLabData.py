import subprocess
import shlex
import datetime
from tarfile import TarFile
import os
import datetime
import iso8601
import geoip2.database
import ipaddress
import time
import paho.mqtt.client as mqtt
now = datetime.datetime.now()
dayoffset=20

# Output path for all the data and directory structures
outpath='/home/pi/data/'

# Filename to store the directory structure on Google Cloud
fnameout='dirout.txt'

# Directory with the gsutil executable
gsutildir = '/home/pi/gsutil/'

# The NDT server name we will get data from
focusserver = 'atl01'

# MQTT server ip address
serverIP = "192.168.1.249"

# MQTT Topic name for which the server is listening
mqtttopic = "ndt"

#Data date: (20160504 run on 6/21/2016)
datayear = 2016
datamonth = 05
dataday = 05

print "Data year: %d" % now.year
print "Data month: %d" % now.month
print "Data day: %d" % dataday


#Log file output format:
#
# day, serverip, clientip, clientport, clientlat, clientlon, countrtt, sumrtt, avgrtt, clientsub
#
#

############################################################
#
# Set up the MQTT publisher.
#
############################################################

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("$SYS/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def on_publish(mqttc, obj, mid):
    print("mid: "+str(mid))
    pass

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish

client.connect(serverIP, 1883, 60)

############################################################
#
#
# Step 1: Find out what files are on the Google Store
# Store that list in a filename dirout.txt (which gets replaced daily
# This file will be scheduled (via cron) to run once a day
#
############################################################


cmd=gsutildir + 'gsutil ls -l gs://m-lab/ndt/{0:02d}/{1:02d}/{2:02d}'.format(datayear, datamonth,dataday) 
print cmd
cmd = cmd.split()

#record a log file for each day's data
logfile='metadata{0:02d}{1:02d}{2:02d}.log'.format(now.year, now.month,now.day-dayoffset)

with open(outpath+fnameout,'w') as fout:
    #context manager is OK since `call` blocks :)
    subprocess.call(cmd,stdout=fout)

############################################################
#
#
# Step 2: read back in this file and parse through the lines
# We are looking for the atl01 files to download to temporary locations
# We'll then need to parse those files
#
#
############################################################

filestoopen = list()
with open(outpath+fnameout, 'r') as fin:
    for line in fin:
        postline=line.partition('-mlab1-')[2]
        server=postline.partition('-ndt')[0]
        if server == focusserver:
            filename1 = line.partition('Z ')[2]
            filename = filename1.partition('\n')[0]
            filestoopen.append(filename.strip())

############################################################
#
#
# Step 3: We now have the list of zip files to parse through to look for data
# So we download one file from the list, unzip it, keeping the
# meta files, then delete it
#
############################################################

client.loop_start()
 
for zipfile in filestoopen:
    print "Fetching " + zipfile + " "

    fnameout='temp/tempzip.tgz'
	
    cmd=gsutildir + 'gsutil cp ' + zipfile + " " + outpath + fnameout
    cmd = cmd.split()
    subprocess.call(cmd)

    
    temppath = 'temp/templog/'
    if (not os.path.isdir(outpath+temppath)):     
        os.mkdir(outpath+temppath)
    count = 0
    
    datalogs = list() #The list of meta files that we need to parse
    with TarFile.open(outpath+fnameout, 'r') as mytar:
        for member in mytar.getmembers():
            if ".meta" in member.name or ".log" in member.name:
                member.name = os.path.basename(member.name)
                print member.name
                count = count + 1
                datalogs.append(member.name)
                mytar.extract(member, path=outpath+temppath)
    #delete the temporary tar file
    os.remove(outpath+fnameout)
    print "{} entries to process".format(count)

    raw_input("Press Enter to continue.")
    
############################################################
#
#
# Step 4: Parse the meta file to get the information
# about the NDT query
#
############################################################

    for metafile in datalogs:
        try:
            with open(outpath+temppath+metafile , 'r') as infile:
                day = ''
                serverip = ''
                clientip = ''
                clientport = 0
                clientlat = 0.0
                clientlon = 0.0
                countrtt = 0
                sumrtt = 0
                avgrtt = 0.0
                clientsub = ''
                
                for line in infile:
                    if line.split(" ")[0] == "Date/Time:":
                        dt = iso8601.parse_date(line.split(' ')[1].strip())
                        day=datetime.datetime.isoformat(dt)
                    elif line.split("_snaplog")[0] == 'c2s':
                        clientport = line.split(".c2s")[0].split(":")[4].strip()
                    elif line.split(":")[0] == "server IP address":
                        serverip = line.split(":")[1].strip()
                        if serverip == '':
                            serverip = '4.71.254.147'  
                    elif line.split(":")[0] == "client IP address":
                        clientip = line.split(":")[1].strip()
                    elif line.split(" data:")[0] == "Summary":
                        spline = line.split(',')
                        sumrtt = int(spline[4])
                        countrtt = int(spline[5])
                        if countrtt > 0:
                            avgrtt = float(sumrtt) /countrtt

            #delete the temporary metafile
            os.remove(outpath+temppath+metafile)                

            #Get the geocode         
            reader = geoip2.database.Reader(outpath + 'GeoLite2-City.mmdb')
            response = reader.city(clientip)
            clientlat = response.location.latitude
            clientlon = response.location.longitude
            reader.close()

            #Format the client subnet address
            clientsub = str(ipaddress.IPv4Address((int(ipaddress.IPv4Address(unicode(clientip))))&(2**32-2**8)))

            #Format the output string
            output = day + "," + serverip  
            output += "," + clientip+"," + str(clientport)
            output += "," + "{:6f}".format(clientlat) +"," + "{:6f}".format(clientlon)
            output += "," + str(countrtt) + "," + str(sumrtt) + "," + "{:6f}".format(avgrtt) + ',' + clientsub
            
            print output

            #Save the output string to the log file
            with open(outpath+logfile, "a") as logline:
                logline.write(output)

            #Send the string to the MQTT messenger
            (rc, mid) = client.publish(mqtttopic, output, qos=0)
                
        except:
            print "Error on reading"
            pass
        
        #Wait one second before moving to the next data point
        time.sleep(1)
        
client.loop_stop()

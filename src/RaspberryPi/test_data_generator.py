
import datetime

import random
import string

# Save path for the test data
outpath='./'

# File name for the temp data
templog = 'tempdata.log'

# Number of data points to generate
npoints = 1000


# Get the start time for our random test data
now = datetime.datetime.now()

for i in range(npoints):
    day = ''
    serverip = 'serverip'
    clientip = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
    clientport = random.randint(10000,99999)
    clientlat = random.uniform(-90,90)
    clientlon = random.uniform(-180,180)
    countrtt = random.randint(10,1000)
    sumrtt = random.randint(1000,100000)
    avgrtt = float(sumrtt)/countrtt
    if random.random() > 0.95:
        avgrtt *= 10
    clientsub = ''.join(random.choice(string.ascii_uppercase))

    
    start = iso8601.parse_date('2015-01-01')
    end = iso8601.parse_date('2016-06-04')
    dt = start + datetime.timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
    day=datetime.datetime.isoformat(dt)


    output = day + "," + serverip  
    output += "," + clientip+"," + str(clientport)
    output += "," + "{:6f}".format(clientlat) +"," + "{:6f}".format(clientlon)
    output += "," + str(countrtt) + "," + str(sumrtt) + "," + "{:6f}".format(avgrtt) + ',' + clientsub
#    print output

    

    with open(outpath + templog , 'a') as logfile:
        output += "\n"
        logfile.writelines(output)



import paho.mqtt.client as mqtt
import time

serverIP = "192.168.1.249"

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

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
#client.loop_forever()

client.loop_start()
for k in range(10):
    print k
    print("tuple")
    (rc, mid) = client.publish("ndt", "tuple", qos=0)
    print("class")
    infot = client.publish("ndt", "class", qos=0)
    time.sleep(1)

client.loop_stop()

package storm;

import java.util.LinkedList;
import java.util.Map;

import org.apache.kafka.common.utils.Utils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class MQTTSpout implements MqttCallback, IRichSpout {
	MqttClient client;
	SpoutOutputCollector _collector;
	LinkedList<String> messages;

	String _broker_url;
	String _client_id;
	String _topic;

	public MQTTSpout(String broker_url, String clientId, String topic) {
		_broker_url = broker_url;
		_client_id = clientId;
		_topic = topic;
		messages = new LinkedList<String>();
	}

	public void messageArrived(String topic, MqttMessage message)
			throws Exception {
		messages.add(message.toString());
	}

	public void connectionLost(Throwable cause) {
	}

	public void deliveryComplete(IMqttDeliveryToken token) {
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		try {
			client = new MqttClient(_broker_url, _client_id);
			client.connect();
			client.setCallback(this);
			client.subscribe(_topic);

		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	public void close() {
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public void nextTuple() {
		while (!messages.isEmpty()) {
			String output = messages.poll();
			
		    String[] parts = output.split(",");
		    String clientsub = parts[9];
			
			_collector.emit(new Values(clientsub.trim(), output.trim()));
			//System.out.println(output);
		}
	}

	public void ack(Object msgId) {
	}

	public void fail(Object msgId) {
	}

    public void declareOutputFields(
            OutputFieldsDeclarer outputFieldsDeclarer)
    {
          // tell storm the schema of the output tuple for this spout
		  //schema:  a single text line with: day, serverip,clientip,clientport,clientlat,clientlon
			//	countrtt,sumrtt,avgrtt,clientsub
          outputFieldsDeclarer.declare(new Fields("clientsub", "message"));
    }
    
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
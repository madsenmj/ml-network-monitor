package storm;

import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.common.utils.Utils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.joda.time.DateTime;
import org.json.JSONArray;

import clojure.lang.Util;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class RandomPBISpout implements IRichSpout {
	MqttClient client;
	SpoutOutputCollector _collector;
	DateTime lastdate;
	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	Random rand = new Random();
	
	public RandomPBISpout() {
		
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		
        long t1 = System.currentTimeMillis() + rand.nextInt();
        lastdate = new DateTime(t1);
	}

	public void close() {
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public void nextTuple() {
    	
    	String[] outliers = {"false","true"};
    	
    	int min = 0;
    	int max = 255;
    	
    	Integer random1 = rand.nextInt(max - min) + min;
    	Integer random2 = rand.nextInt(max - min) + min;
    	Integer random3 = rand.nextInt(max - min) + min;
    	Integer random4 = rand.nextInt(max - min) + min;

    	Float avgrtt= rand.nextFloat()*1000f;
    	String clientip = random1.toString() + "." + random2.toString() + "." + random3.toString() + "." + random4.toString();
    	Float clientlat = (rand.nextFloat()-0.5f)*180;
    	Float clientlon = (rand.nextFloat()-0.5f)*360;
    	String clientsub = random1.toString() + "." + random2.toString() + "." + random3.toString() + ".0" ;
        
        long t2 = lastdate.getMillis() + 2 * 60 * 1000 + rand.nextInt(60 * 1000) + 1;
        DateTime d2 = new DateTime(t2);
        lastdate = d2;
    	String datadate = d2.toString();
    	String outlier = outliers[rand.nextInt(2)];
    	Float queryfreq = rand.nextFloat()/10f;
 		_collector.emit(new Values(datadate, clientip, clientlat, clientlon, avgrtt, outlier, queryfreq, clientsub));
		Utils.sleep(3000);
		
	}

	public void ack(Object msgId) {
	}

	public void fail(Object msgId) {
	}

    public void declareOutputFields(
            OutputFieldsDeclarer outputFieldsDeclarer)
    {
          // tell storm the schema of the output tuple for this spout
          outputFieldsDeclarer.declare(new Fields("daytime","clientip","clientlat","clientlon",
        		  									"avgrtt","outlier","datafreq","clientsub"));
          
    }
    
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	String randomString( int len ){
		   StringBuilder sb = new StringBuilder( len );
		   for( int i = 0; i < len; i++ ) 
		      sb.append( AB.charAt( rand.nextInt(AB.length()) ) );
		   return sb.toString();
		}

}
package com.allegient.storm;

import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.ws.rs.core.MediaType;

import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import twitter4j.DirectMessage;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class TwitterBolt extends BaseBasicBolt {
	    OutputCollector outputCollector;
	    Properties topologyConfig;
	    Twitter twitter;
	    

   		//Variables we'll get from the properties file on prepare
   		String recipient;
   		
	    TwitterBolt(Twitter tinstance, Properties top ){
	    	twitter = tinstance;
	    	topologyConfig = top;
    		
	    	recipient = topologyConfig.getProperty("twitter.message.recipient");
	    	
	    }
	    
	    
	   /**
		 * 
		 */
		private static final long serialVersionUID = -8273457807673293419L;

	    public void prepare(
		        Map                     map,
		        TopologyContext         topologyContext,
		        OutputCollector         collector)
		    {
		      // save the output collector for emitting tuples
	    		outputCollector = collector;
	    		

		    }
		
		
		public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {

			ofDeclarer.declare(new Fields("string"));
			
	   }


	   public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
   			
		    Boolean outlier = Boolean.parseBoolean((String) tuple.getValueByField("outlier"));
		    
		    if (outlier){
			   	StringBuilder newmessage = new StringBuilder();
			   	newmessage.append("There was a possible problem at ");
			   	newmessage.append(String.format("Lat: %2f",tuple.getValueByField("clientlat")));
			   	newmessage.append(String.format(",Lon: %2f",tuple.getValueByField("clientlon")));
			   	newmessage.append(" on " + tuple.getValueByField("daytime"));
			   	newmessage.append(" from " + tuple.getValueByField("clientsub"));
			   	newmessage.append(".");
		    	
		    	System.out.println(newmessage.toString());
		    	outputCollector.emit(new Values(newmessage.toString()));
		    	

				try {
				    DirectMessage message = twitter.sendDirectMessage(recipient, newmessage.toString());
				    System.out.println("Direct message successfully sent to " + message.getRecipientScreenName());
				} catch (TwitterException te) {
				    te.printStackTrace();
				    System.out.println("Failed to send a direct message: " + te.getMessage());
				}
		    	
		    }

	   }
	}
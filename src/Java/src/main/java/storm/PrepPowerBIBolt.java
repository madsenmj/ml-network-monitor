package storm;

import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.ws.rs.core.MediaType;

import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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


public class PrepPowerBIBolt extends BaseBasicBolt {
	    OutputCollector outputCollector;
	    String accessToken;
	    String refreshToken;
	    DateTime expireTime;
	    Properties topologyConfig;

   		LinkedList<JSONObject> rowList; //The JSON rows we sent to Power BI
   		Integer listlength;

   		//Variables we'll get from the properties file on prepare
   		String powerbiuri;
   		String powerbidataset;
   		String powerbitable;
   		
	    PrepPowerBIBolt(String acTo, String refTo, DateTime exp, Properties top ){
	    	accessToken = acTo;
	    	refreshToken = refTo;
	    	expireTime = exp;
	    	topologyConfig = top;

    		// Establish the communications client
	   		Client client = Client.create();
    		listlength = Integer.parseInt(topologyConfig.getProperty("powerbi.list.length"));
    		powerbiuri = topologyConfig.getProperty("powerbi.uri");
    		powerbidataset = topologyConfig.getProperty("powerbi.dataset.id");
    		powerbitable = topologyConfig.getProperty("powerbi.dataset.table.name");
	    	
    		
    		
    		
    		// Delete Previous Rows from the Power BI table
    		 
    		WebResource wr = client.resource(powerbiuri + "/datasets" + powerbidataset + "/tables" + powerbitable + "/rows");
    		String response = wr.accept(MediaType.TEXT_PLAIN)
    			  .header("Authorization", "Bearer " + accessToken)
    			  .delete(String.class);
    		
    		rowList = new LinkedList<JSONObject>();
    		
    		/*
    		for (int k=0; k<20; k++){
    		
    		
    		//Assemble the data for output
		   	JSONObject newdata=new JSONObject();
	    	JSONObject row = new JSONObject();
	    	try {
				row.put("AvgRTT", 0.0);
		    	row.put("ClientIP", "XX");
		    	row.put("ClientLat",0.0);
		    	row.put("ClientLon", 0.0);
		    	row.put("ClientSub",0.0);
		    	row.put("Date", "1995-01-01T01:01:01");
		    	row.put("Outlier", "false");
		    	row.put("QueryFreq", 0.0);
		    	
		    	JSONArray allrows = new JSONArray();
		    	for (int j=0;j<10000;j++){
		    		allrows.put(row);
		    	}
		    	
		    	newdata.put("rows",allrows);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    		
	   		WebResource wr = client.resource(topologyConfig.getProperty("powerbi.uri")+ "/datasets" + topologyConfig.getProperty("powerbi.dataset.id") + "/tables" + topologyConfig.getProperty("powerbi.dataset.table.name")+ "/rows");
	   		String response = wr.accept(MediaType.TEXT_PLAIN)
				  .header("Authorization", "Bearer " + accessToken)
				  .post(String.class,newdata.toString());
    		
    		}
    		
    		*/
    		
    		
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
	       	
		   //Assemble the data for output
		   	JSONObject newdata=new JSONObject();
	    	JSONObject row = new JSONObject();
	    	try {
				row.put("AvgRTT", tuple.getValueByField("avgrtt"));
		    	row.put("ClientIP", tuple.getValueByField("clientip"));
		    	row.put("ClientLat",tuple.getValueByField("clientlat" ));
		    	row.put("ClientLon", tuple.getValueByField("clientlon"));
		    	row.put("ClientSub",tuple.getValueByField("clientsub"));
		    	row.put("Date", tuple.getValueByField("daytime"));
		    	row.put("Outlier", tuple.getValueByField("outlier"));
		    	row.put("QueryFreq", tuple.getValueByField("datafreq"));
		    	
		    	//Maintain our list of active points to push to Power BI
		    	rowList.add(row);
		    	if (rowList.size() > listlength){
		    		rowList.pop();	
		    	}
		    	JSONArray allrows = new JSONArray();
		    	for (JSONObject j : rowList){
		    		allrows.put(j);
		    	}
		    	
		    	newdata.put("rows",allrows);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
    		// Establish the communications client
	   		Client client = Client.create();
	    	
	   		//delete the past data
	   		
    		WebResource wr = client.resource(powerbiuri + "/datasets" + powerbidataset + "/tables" + powerbitable + "/rows");
    		String response = wr.accept(MediaType.TEXT_PLAIN)
    			  .header("Authorization", "Bearer " + accessToken)
    			  .delete(String.class);
	   		
	   		
			// POST the active rows
	   		wr = client.resource(topologyConfig.getProperty("powerbi.uri")+ "/datasets" + topologyConfig.getProperty("powerbi.dataset.id") + "/tables" + topologyConfig.getProperty("powerbi.dataset.table.name")+ "/rows");
	   		response = wr.accept(MediaType.TEXT_PLAIN)
				  .header("Authorization", "Bearer " + accessToken)
				  .post(String.class,newdata.toString());

	   }
	}
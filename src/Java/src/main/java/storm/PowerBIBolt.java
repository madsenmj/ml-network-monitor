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


public class PowerBIBolt extends BaseBasicBolt {
	    OutputCollector outputCollector;
	    String accessToken;
	    String refreshToken;
	    DateTime expireTime;
	    Properties topologyConfig;

   		LinkedList<JSONObject> rowList; //The JSON rows we sent to Power BI
   		LinkedList<JSONObject> nextList;
   		Integer listlength;

   		//Variables we'll get from the properties file on prepare
   		String powerbiuri;
   		String powerbidataset;
   		String powerbitable;
   		
	    PowerBIBolt(String acTo, String refTo, DateTime exp, Properties top ){
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
    		nextList = new LinkedList<JSONObject>();
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
   			// Establish the communications client
	   		Client client = Client.create();
	   		WebResource wr = null;
	   		String response = null;
	   		//Assemble the data for output
		   	JSONObject newdata=new JSONObject();
	    	JSONObject row = new JSONObject();
	    	JSONArray allrows = new JSONArray();
	    	
	    	if (nextList.size() >= listlength){
	    		//We need to wipe the current data and reset.
	    		System.out.println("\n**************Resetting PowerBI Data ****************\n");
	    		
	    		
	    		//Now we need to wipe all the data, post the nextList, then reset
	    		
	    		//Prepare the next data set
		    	for (JSONObject j : nextList){
		    		allrows.put(j);
		    	}
		    	try {
					newdata.put("rows",allrows);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	
		   		//delete the past data
	    		wr = client.resource(powerbiuri + "/datasets" + powerbidataset + "/tables" + powerbitable + "/rows");
	    		response = wr.accept(MediaType.TEXT_PLAIN)
	    			  .header("Authorization", "Bearer " + accessToken)
	    			  .delete(String.class);
		    	
				// POST the past rows
		   		wr = client.resource(topologyConfig.getProperty("powerbi.uri")+ "/datasets" + topologyConfig.getProperty("powerbi.dataset.id") + "/tables" + topologyConfig.getProperty("powerbi.dataset.table.name")+ "/rows");
				response = wr.accept(MediaType.TEXT_PLAIN)
					  .header("Authorization", "Bearer " + accessToken)
					  .post(String.class,newdata.toString());
	    			    		
	    		rowList.clear();
	    		rowList=nextList;
	    		nextList.clear();
	    		
	    	}

	    	try {
				row.put("AvgRTT", tuple.getValueByField("avgrtt"));
		    	row.put("ClientIP", tuple.getValueByField("clientip"));
		    	row.put("ClientLat",tuple.getValueByField("clientlat" ));
		    	row.put("ClientLon", tuple.getValueByField("clientlon"));
		    	row.put("ClientSub",tuple.getValueByField("clientsub"));
		    	row.put("Date", tuple.getValueByField("daytime"));
		    	row.put("Outlier", tuple.getValueByField("outlier"));
		    	row.put("QueryFreq", tuple.getValueByField("datafreq"));

		    	if (rowList.size() < listlength){
		    		System.out.println("\n**************Building List 1:" + String.valueOf(rowList.size()) +" ****************\n");
		    		
		    		//We aren't at the maximum number of points, so we just need to add a row
			    	rowList.add(row);
		    		allrows.put(row);
			    	newdata.put("rows",allrows);
		    		wr = client.resource(topologyConfig.getProperty("powerbi.uri")+ "/datasets" + topologyConfig.getProperty("powerbi.dataset.id") + "/tables" + topologyConfig.getProperty("powerbi.dataset.table.name")+ "/rows");
					response = wr.accept(MediaType.TEXT_PLAIN)
							  .header("Authorization", "Bearer " + accessToken)
							  .post(String.class,newdata.toString());
		    		
		    	} else if (nextList.size() < listlength){
		    		System.out.println("\n**************Building List 2:" + String.valueOf(nextList.size()) +" ****************\n");
		    		//At this point we are adding on to the next list
			    	nextList.add(row);
		    		allrows.put(row);
			    	newdata.put("rows",allrows);
		    		wr = client.resource(topologyConfig.getProperty("powerbi.uri")+ "/datasets" + topologyConfig.getProperty("powerbi.dataset.id") + "/tables" + topologyConfig.getProperty("powerbi.dataset.table.name")+ "/rows");
					response = wr.accept(MediaType.TEXT_PLAIN)
							  .header("Authorization", "Bearer " + accessToken)
							  .post(String.class,newdata.toString());
		    	
		    	}

			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	

	    	


	   }
	}
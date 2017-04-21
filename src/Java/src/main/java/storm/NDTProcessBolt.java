package storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NDTProcessBolt extends BaseRichBolt{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1277707373121916751L;
	// To keep track of the latest data coming in
	private Map<String, NDTData> latestEntries = new HashMap<String, NDTData>();
	// To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;
    
	NDTProcessBolt(Map<String, NDTData> prevRes){	
	      latestEntries = prevRes;
	}

    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         outputCollector)
    {

      // save the collector for emitting tuples
      collector = outputCollector;


    }

    public void execute(Tuple tuple)
    {
      String message = tuple.getStringByField("message");
      String clientsub = tuple.getStringByField("clientsub");
      //schema:  day, serverip,clientip,clientport,clientlat,clientlon
      //	countrtt,sumrtt,avgrtt,clientsub
      String[] parts = message.split(",");
      
      
      String day = parts[0];
      Float avgrtt = Float.parseFloat(parts[8]);
      
      System.out.println("Working on Clientsub: " + clientsub);
      
      // check if the clientsub is present in the map
      if (latestEntries.get(clientsub) == null) {
    	  System.out.println("Adding new entry.");
    	  // not present, add the clientsub
    	  //(String day, Float avgrtt, Integer ninsub, Float avginsub, Float sigmainsub, Float datafreq)
    	  latestEntries.put(clientsub, new NDTData(day, 1, avgrtt, avgrtt, 0f, 0f));
      } else {
    	  System.out.println("Prior data: " + latestEntries.get(clientsub).toString());
    	  // already there, update the parameters
    	  latestEntries.put(clientsub, new NDTData(day, avgrtt, latestEntries.get(clientsub)));
      }
      
      System.out.println("Updated data: " + latestEntries.get(clientsub).toString());
      
      
      //TODO: update this to emit all the data, not just the new values
      							/*
      	 					 	"daytime", 
        						"serverip", 
        						"clientip", 
        						"clientport", 
        						"clientlat", 
        						"clientlon",
        						"countrtt", 
        						"sumrtt",
        						"avgrtt",
        						"ninsub",
        						"avginsub",
        						"sigmainsub",
        						"datafreq",
        						"outlier",
        						"dataday",
        						"clientsub"
      						 	*/

      String outputstr = "";
      for (int j = 0; j < 9; j++){
    	  outputstr += parts[j] + ",";
      }
      outputstr += latestEntries.get(clientsub).toFields();
      outputstr += "," + clientsub;
      
      collector.emit(new Values(clientsub, outputstr.trim()));

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
      // tell storm the schema of the output tuple for this spout

      outputFieldsDeclarer.declare(new Fields("clientsub","ndtdata"));

    }
}
	
	

package com.allegient.storm;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class HiveEmitBolt extends BaseBasicBolt {


   /**
	 * 
	 */
	private static final long serialVersionUID = -8273457807673193419L;

	
	
	public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
       ofDeclarer.declare(new Fields(	"daytime", 
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
    		   							"clientsub"));
   }


   public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
       
	   
	   String[] ndtdata = tuple.getStringByField("ndtdata").split(",");
          
	   /*
		 Hive Table Schema
		0 "daytime", 
		1 "serverip", 
		2 "clientip", 
		3 "clientport", 
		4 "clientlat", 
		5 "clientlon",
		6 "countrtt", 
		7 "sumrtt",
		8 "avgrtt",
		9 "ninsub",
		10 "avginsub",
		11 "sigmainsub",
		12 "datafreq",
		13 "outlier",
		14 "dataday",
		15 "clientsub"
		*/
	   
	Values values = new Values( ndtdata[0], 
				ndtdata[1], 
				ndtdata[2], 
				Integer.parseInt(ndtdata[3]),
				Float.parseFloat(ndtdata[4]), 
				Float.parseFloat(ndtdata[5]), 
				Integer.parseInt(ndtdata[6]), 
				Integer.parseInt(ndtdata[7]),
				Float.parseFloat(ndtdata[8]), 
				Integer.parseInt(ndtdata[9]), 
				Float.parseFloat(ndtdata[10]), 
				Float.parseFloat(ndtdata[11]), 
				Float.parseFloat(ndtdata[12]), 
				Integer.parseInt(ndtdata[13]), 
				ndtdata[14],
				ndtdata[15].trim());
       outputCollector.emit(values); 
   }
}
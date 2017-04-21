package storm;

import java.io.UnsupportedEncodingException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class PowerBIEmitBolt extends BaseBasicBolt {


   /**
	 * 
	 */
	private static final long serialVersionUID = -8273457807673193419L;


	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("daytime","clientip","clientlat","clientlon",
					"avgrtt","outlier","datafreq","clientsub"));
   }


   public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
       
	   
	   String[] ndtdata = tuple.getStringByField("ndtdata").split(",");
          
	   /*
	    * Data coming in as a string with this schema:
	    * 
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
	   
	   
	   /*
	    * Desired output:
	    * "daytime","clientip","clientlat","clientlon","avgrtt","outlier","datafreq","clientsub"
	    * 
	    * */
	   String[] outliers = {"false","true"};
	   
	   Values values = new Values(ndtdata[0], 
								ndtdata[2], 
								Float.parseFloat(ndtdata[4]), 
								Float.parseFloat(ndtdata[5]), 
								Float.parseFloat(ndtdata[8]), 
								outliers[Integer.parseInt(ndtdata[13])], 
								Float.parseFloat(ndtdata[12]),
								ndtdata[15]);
       outputCollector.emit(values);
   
   }
}
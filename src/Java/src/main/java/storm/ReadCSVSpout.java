package com.allegient.storm;

import java.io.FileReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import au.com.bytecode.opencsv.CSVReader;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ReadCSVSpout extends BaseRichSpout {
	  private final String fileName;
	  private final char separator;
	  private boolean includesHeaderRow;
	  private SpoutOutputCollector _collector;
	  private CSVReader reader;
	  private AtomicLong linesRead;

	  public ReadCSVSpout(String filename, char separator, boolean includesHeaderRow) {
	    this.fileName = filename;
	    this.separator = separator;
	    this.includesHeaderRow = includesHeaderRow;
	    linesRead=new AtomicLong(0);
	  }

	  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	    _collector = collector;
	    try {
	      reader = new CSVReader(new FileReader(fileName), separator);
	      // read and ignore the header if one exists
	      if (includesHeaderRow) reader.readNext();
	    } catch (Exception e) {
	      throw new RuntimeException(e);
	    }
	  }
/*
 * 										0"daytime", 
    		   							1"serverip", 
    		   							2"clientip", 
    		   							3"clientport", 
    		   							4"clientlat", 
    		   							5"clientlon",
    		   							6"countrtt", 
    		   							7"sumrtt",
    		   							8"avgrtt",
    		   							9"ninsub",
    		   							10"avginsub",
    		   							11"sigmainsub",
    		   							12"datafreq",
    		   							13"outlier",
    		   							14"dataday",
    		   							15"clientsub"));
 * */

	  public void nextTuple() {
	    try {
	      String[] ndtdata = reader.readNext();
	      if (ndtdata != null) {
	        long id=linesRead.incrementAndGet();
	        
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
	       _collector.emit(values); 

	      }
	      else
	        System.out.println("Finished reading file, "+linesRead.get()+" lines read");
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	  }

	  @Override
	  public void ack(Object id) {
	  }

	  @Override
	  public void fail(Object id) {
	    System.err.println("Failed tuple with id "+id);
	  }


	  public void declareOutputFields(OutputFieldsDeclarer declarer) {
	       declarer.declare(new Fields(	"daytime", 
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


	}
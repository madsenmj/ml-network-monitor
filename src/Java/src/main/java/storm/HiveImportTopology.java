package storm;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.JsonRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.mqtt.common.MqttOptions;
import org.apache.storm.mqtt.mappers.StringMessageMapper;
import org.apache.storm.mqtt.spout.MqttSpout;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class HiveImportTopology extends BaseTopology{
    private static final String READ_CSV_SPOUT_ID = "readCSVSpout";
    private static final String HIVE_BOLT_ID = "hiveBolt";
	
    

    
	public HiveImportTopology(String configFileLocation) throws Exception {
		super(configFileLocation);

	} 
    
       
    public void configureReadCSVSpout(TopologyBuilder builder){
    	String file = "/root/data/ndtsubdata50-40.csv";
    	ReadCSVSpout rcsvs = new ReadCSVSpout(file,',',false);
    	builder.setSpout(READ_CSV_SPOUT_ID, rcsvs,1);
    }
        
    
    
    public void configureHiveBolt(TopologyBuilder builder){
    	// Hive connection configuration
        String metaStoreURI = topologyConfig.getProperty("hive.metaStoreURI");
        String dbName =  topologyConfig.getProperty("hive.database");
        String tblName = topologyConfig.getProperty("hive.table");
        
        // Fields for possible partition
        String partNames = "clientsub";
        // Fields for possible column data
        String[] colNames = {	"daytime", 
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
        						"dataday"};
        // Record Writer configuration
        
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames))
                .withPartitionFields(new Fields(partNames));

        HiveOptions hiveOptions;
        hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper)
                .withTxnsPerBatch(100)
                .withBatchSize(10)
                .withIdleTimeout(10);
                //.withKerberosKeytab(path_to_keytab)
                //.withKerberosPrincipal(krb_principal);
        builder.setBolt(HIVE_BOLT_ID, new HiveBolt(hiveOptions)).shuffleGrouping(READ_CSV_SPOUT_ID);
    	
    }
    
    private void buildAndSubmit() throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        configureReadCSVSpout(builder);
        
        configureHiveBolt(builder);
        
        
        Config conf = new Config();
        conf.setDebug(false);

        /* Uncomment when we want to submit to the actual cluster */
        //StormSubmitter.submitTopology("mqtt-event-processor",conf, builder.createTopology());
        
        /*Or run locally with a timeout */
        // create the local cluster instance
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mqtt-event-processor",conf, builder.createTopology());
       
        //**********************************************************************
        // let the topology run for 30 seconds. note topologies never terminate!
        Thread.sleep(1800000);
        //**********************************************************************

        // we are done, so shutdown the local cluster
        cluster.shutdown();
        
    }

    public static void main(String[] str) throws Exception
    {

        
        String configFileLocation = "mqtt_event_topology.properties";
        HiveImportTopology mqttTopology 
                = new HiveImportTopology(configFileLocation);
        mqttTopology.buildAndSubmit();
        

    }

}

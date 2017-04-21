package storm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
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


public class MQTTTopology extends BaseTopology{
    private static final String MQTT_SPOUT_ID = "mqttspout";
    private static final String NDTPROCESS_BOLT_ID = "ndtprocessBolt"; 

	
	public MQTTTopology(String configFileLocation) throws Exception {
		super(configFileLocation);

	} 
    
    public void configureMQTTSpout(TopologyBuilder builder) 
    {
    	String broker_url = topologyConfig.getProperty("mqtt.broker.url");
    	String clientId = topologyConfig.getProperty("mqtt.clientid");
    	String topic = topologyConfig.getProperty("mqtt.topic");
    	
        MQTTSpout mqttSpout = new MQTTSpout(broker_url, clientId,topic);
        builder.setSpout(MQTT_SPOUT_ID, mqttSpout,Integer.parseInt(topologyConfig.getProperty("spout.thread.count")));

    }
    
    public void configureNDTProcessBolt(TopologyBuilder builder) throws SQLException{
    	Map<String, NDTData> latestEntries = new HashMap<String, NDTData>();
    	String dataTable = topologyConfig.getProperty("hive.table");
    	String hiveUser = topologyConfig.getProperty("hive.user");
    	String hiveURL = topologyConfig.getProperty("hive.url");
    	String hiveDriver = topologyConfig.getProperty("hive.driver");
    	
		try {
			  Class.forName(hiveDriver);
		} catch (ClassNotFoundException e) {
			  e.printStackTrace();
			  System.exit(1);
		}
		
		System.out.println("Opening JDBC Hive connection....");
		Connection connect = DriverManager.getConnection(hiveURL, hiveUser,"");
		System.out.println("Connected. Creating statement....");
		Statement state = connect.createStatement();
		// Query to Use:
		
		String query = "select b.clientsub as clientsub, "
					+ "b.day as day, b.ninsub as ninsub, "
					+ "b.avgrtt as avgrtt, b.avginsub as avginsub, "
					+ "b.sigmainsub as sigmainsub, b.datafreq as datafreq "
					+ "from (select clientsub, max(day) as day from "
					+ dataTable + " group by clientsub) a left outer join "
					+ dataTable + " b on (a.clientsub=b.clientsub  and a.day=b.day)";
		System.out.println("Fetching prior data...");
		ResultSet res = state.executeQuery(query);
		while (res.next()) {
			String clientsub = res.getString(1);
			//System.out.println(res.getString(1) + "," + res.getString(2));
			//NDTData(String day, Float avgrtt, Integer ninsub, Float avginsub, Float sigmainsub, Float datafreq)
			latestEntries.put(clientsub, new NDTData(res.getString(2),res.getInt(3),res.getFloat(4)
							,res.getFloat(5),res.getFloat(6),res.getFloat(7)));
		}
		NDTProcessBolt ndtProcessBolt = new NDTProcessBolt(latestEntries);
        builder.setBolt(NDTPROCESS_BOLT_ID, ndtProcessBolt,Integer.parseInt(topologyConfig.getProperty("spout.thread.count")))
        		.shuffleGrouping(MQTT_SPOUT_ID);
    }
    

    
    private void buildAndSubmit() throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        configureMQTTSpout(builder);

        configureNDTProcessBolt(builder);
        
        Config conf = new Config();
        conf.setDebug(true);
        
        /* Uncomment when we want to submit to the actual cluster */
        //StormSubmitter.submitTopology("mqtt-event-processor",conf, builder.createTopology());
        
        /*Or run locally with a timeout */
        // create the local cluster instance
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mqtt-event-processor",conf, builder.createTopology());
       
        //**********************************************************************
        // let the topology run for 30 seconds. note topologies never terminate!
        Thread.sleep(60000);
        //**********************************************************************

        // we are done, so shutdown the local cluster
        cluster.shutdown();
        
    }

    public static void main(String[] str) throws Exception
    {

        
        String configFileLocation = "mqtt_event_topology.properties";
        MQTTTopology mqttTopology 
                = new MQTTTopology(configFileLocation);
        mqttTopology.buildAndSubmit();
        

    }

}

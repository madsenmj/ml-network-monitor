package com.allegient.storm;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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

import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
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
import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONObject;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class MQTTHivePBITwitterTopology extends BaseTopology{
    private static final String MQTT_SPOUT_ID = "mqttspout";
    private static final String NDTPROCESS_BOLT_ID = "ndtprocessBolt";
    private static final String HIVEEMIT_BOLT_ID = "hiveemitBolt";
    private static final String HIVE_BOLT_ID = "hiveBolt";
    private static final String POWERBI_BOLT_ID = "powerBIBolt";
    private static final String POWERBIEMIT_BOLT_ID = "powerBIEmitBolt";
    private static final String TWITTER_BOLT_ID = "twitterBolt";
    
	public MQTTHivePBITwitterTopology(String configFileLocation) throws Exception {
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

    	String dataTable = topologyConfig.getProperty("hive.table");
    	String hiveUser = topologyConfig.getProperty("hive.user");
    	String hiveURL = topologyConfig.getProperty("hive.url");
    	String hiveDriver = topologyConfig.getProperty("hive.driver");
    	Map<String, NDTData> latestEntries = new HashMap<String, NDTData>();
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
		
		/*
		 * HIVE recent data query:
		select 
		b.clientsub as clientsub, 
		b.daytime as daytime, 
		b.ninsub as ninsub, 
		b.avgrtt as avgrtt, 
		b.avginsub as avginsub, 
		b.sigmainsub as sigmainsub, 
		b.datafreq as datafreq 
		from (select clientsub, max(daytime) as daytime from ndt_test group by clientsub) a 
		left outer join ndt_test b on (a.clientsub=b.clientsub  and a.daytime=b.daytime);
		*/
		
		String query = "select b.clientsub as clientsub, "
					+ "b.daytime as daytime, b.ninsub as ninsub, "
					+ "b.avgrtt as avgrtt, b.avginsub as avginsub, "
					+ "b.sigmainsub as sigmainsub, b.datafreq as datafreq "
					+ "from (select clientsub, max(daytime) as daytime from "
					+ dataTable + " group by clientsub) a left outer join "
					+ dataTable + " b on (a.clientsub=b.clientsub  and a.daytime=b.daytime)";
		
		
		System.out.println("Fetching prior data...");
		ResultSet res = state.executeQuery(query);
		while (res.next()) {
			String clientsub = res.getString(1);
			//System.out.println("Processing: " + res.getString(1) + "," + res.getString(2));
			//NDTData(String day, Integer ninsub,Float avgrtt, Float avginsub, Float sigmainsub, Float datafreq)
			latestEntries.put(clientsub, new NDTData(res.getString(2),res.getInt(3),res.getFloat(4)
							,res.getFloat(5),res.getFloat(6),res.getFloat(7)));
		}
		System.out.println("Found prior clientsubs: " + latestEntries.keySet().toString());
		
		NDTProcessBolt ndtProcessBolt = new NDTProcessBolt(latestEntries);
        builder.setBolt(NDTPROCESS_BOLT_ID, ndtProcessBolt,Integer.parseInt(topologyConfig.getProperty("spout.thread.count")))
        		.fieldsGrouping(MQTT_SPOUT_ID, new Fields("clientsub"));
    }
    
    public void configureHiveEmitBolt(TopologyBuilder builder){
    	builder.setBolt(HIVEEMIT_BOLT_ID, new HiveEmitBolt(),1).fieldsGrouping(NDTPROCESS_BOLT_ID, new Fields("clientsub"));
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
                .withTxnsPerBatch(2)
                .withBatchSize(4)
                .withIdleTimeout(10);
                //.withKerberosKeytab(path_to_keytab)
                //.withKerberosPrincipal(krb_principal);
        builder.setBolt(HIVE_BOLT_ID, new HiveBolt(hiveOptions)).fieldsGrouping(HIVEEMIT_BOLT_ID, new Fields("clientsub"));
    	
    }
    
    public void configurePowerBIEmitBolt (TopologyBuilder builder){
    	builder.setBolt(POWERBIEMIT_BOLT_ID, new PowerBIEmitBolt(),1).fieldsGrouping(NDTPROCESS_BOLT_ID, new Fields("clientsub"));
    }
    
    public void configurePowerBIBolt(TopologyBuilder builder) throws URISyntaxException, ClientProtocolException, IOException, InterruptedException, JSONException{
    	//Gather the resources needed to authenticate Power BI
    	String clientId =topologyConfig.getProperty("powerbi.client.id");
    	String clientSecret =topologyConfig.getProperty("powerbi.client.secret");
    	String username=topologyConfig.getProperty("powerbi.username");
    	String password=topologyConfig.getProperty("powerbi.password");
    	String tenant = topologyConfig.getProperty("powerbi.tenant");
    	String host = topologyConfig.getProperty("powerbi.host");
    	String authority = tenant + topologyConfig.getProperty("powerbi.authority.path");	
       	String resource= topologyConfig.getProperty("powerbi.resource");
    	String input = "resource=" + resource + "&client_id=" + clientId + "&client_secret=" + clientSecret
    						+ "&grant_type=password" + "&username=" + username 
    						+ "&password=" + password + "&scope=openid";
    		    	
    	System.out.println("Authenticating Power BI...");
    	
    	//Get the authentication token
    	CloseableHttpClient httpClient = HttpClients.createDefault();
    	HttpHost targetHost = new HttpHost(host,443,"https");
    	
    	URI uri = new URI(authority);
    	HttpPost httpPost = new HttpPost(uri);
    	
    	httpPost.setHeader("Accept", "application/json");
    	httpPost.setHeader("Content-type", "application/x-www-form-urlencoded");
    	httpPost.setURI(uri);
    	httpPost.setEntity(new StringEntity(input));
    	
    	    	
    	CloseableHttpResponse httpResponse = httpClient.execute(targetHost, httpPost);
    	
    	//Parse the token from the reply
    	String accessToken = null;
    	String refreshToken = null;
    	DateTime tokenExpires = null;
    	try {
    	    System.out.println(httpResponse.getStatusLine());
  	    
    	    JSONObject outputJSON = new JSONObject(EntityUtils.toString(httpResponse.getEntity()));
    	    accessToken =outputJSON.getString("access_token");
    	    refreshToken = outputJSON.getString("refresh_token");
    	    tokenExpires = DateTime.now().plusSeconds(Integer.parseInt(outputJSON.getString("expires_in")));
    	    System.out.println("Access Token Acquired.");
    	    
    	} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
    		httpResponse.close();
    	}

    	httpClient.close();    	
    	
    	PowerBIBolt powerBIBolt = new PowerBIBolt(accessToken,refreshToken,tokenExpires, topologyConfig);
    	builder.setBolt(POWERBI_BOLT_ID, powerBIBolt,1).shuffleGrouping(POWERBIEMIT_BOLT_ID);
    	
    	
    }
    
    public void configureTwitterBolt(TopologyBuilder builder){
    	

    	ConfigurationBuilder cb = new ConfigurationBuilder();
    	cb.setDebugEnabled(true)
    	  .setOAuthConsumerKey(topologyConfig.getProperty("twitter.consumer.key"))
    	  .setOAuthConsumerSecret(topologyConfig.getProperty("twitter.consumer.secret"))
    	  .setOAuthAccessToken(topologyConfig.getProperty("twitter.access.token"))
    	  .setOAuthAccessTokenSecret(topologyConfig.getProperty("twitter.access.token.secret"));

    	TwitterFactory tf = new TwitterFactory(cb.build());
    	Twitter twitter = tf.getInstance();
    	
    	TwitterBolt twitterBolt = new TwitterBolt( twitter, topologyConfig);
    	builder.setBolt(TWITTER_BOLT_ID, twitterBolt,1).shuffleGrouping(POWERBIEMIT_BOLT_ID);

    	
    }
    
    private void buildAndSubmit() throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        configureMQTTSpout(builder);

        configureNDTProcessBolt(builder);
        
        configureHiveEmitBolt(builder);
        
        configureHiveBolt(builder);
        
        configurePowerBIEmitBolt(builder);
        
        configurePowerBIBolt(builder);
        
        configureTwitterBolt(builder);
        
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
        Thread.sleep(180000);
        //**********************************************************************

        // we are done, so shutdown the local cluster
        cluster.shutdown();
        
    }

    public static void main(String[] str) throws Exception
    {
    	System.out.println("NDT Network Data Demo");
    	System.out.println("Martin John Madsen, Ph.D.");
        
        String configFileLocation = "mqtt_event_topology.properties";
        MQTTHivePBITwitterTopology mqttTopology 
                = new MQTTHivePBITwitterTopology(configFileLocation);
        mqttTopology.buildAndSubmit();
        

    }

}

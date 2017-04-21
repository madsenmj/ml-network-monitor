package storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MediaType;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HTTP;
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
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.mqtt.common.MqttOptions;
import org.apache.storm.mqtt.mappers.StringMessageMapper;
import org.apache.storm.mqtt.spout.MqttSpout;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import twitter4j.DirectMessage;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class TwitterTestTopology extends BaseTopology{
    private static final String MQTT_SPOUT_ID = "mqttspout";
    private static final String RPBI_SPOUT_ID = "rpbispout";
    private static final String NDTPROCESS_BOLT_ID = "ndtprocessBolt";
    private static final String HIVEEMIT_BOLT_ID = "hiveemitBolt";
    private static final String HIVE_BOLT_ID = "hiveBolt";
    private static final String POWERBI_BOLT_ID = "powerBIBolt";
    
    private static final String TWITTER_BOLT_ID = "twitterBolt";
	
	public TwitterTestTopology(String configFileLocation) throws Exception {
		super(configFileLocation);

	} 
    
    public void configureRandomPBISpout(TopologyBuilder builder){
    	builder.setSpout(RPBI_SPOUT_ID, new RandomPBISpout(),Integer.parseInt(topologyConfig.getProperty("spout.thread.count")));
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
    	builder.setBolt(TWITTER_BOLT_ID, twitterBolt,1).shuffleGrouping(RPBI_SPOUT_ID);

    	/*
    	String newmessage = "Storm Connection Established.";
    	String recipient = topologyConfig.getProperty("twitter.message.recipient");

		try {
		    DirectMessage message = twitter.sendDirectMessage(recipient, newmessage.toString());
		    System.out.println("Direct message successfully sent to " + message.getRecipientScreenName());
		} catch (TwitterException te) {
		    te.printStackTrace();
		    System.out.println("Failed to send a direct message: " + te.getMessage());
		}
		*/
    	
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
    	builder.setBolt(POWERBI_BOLT_ID, powerBIBolt,1).shuffleGrouping(RPBI_SPOUT_ID);
    	
    	
    }
    
    
    
    private void buildAndSubmit() throws Exception
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        configureRandomPBISpout(builder);
        configureTwitterBolt(builder);
        
        //configurePowerBIBolt(builder);
        
       
        Config conf = new Config();
        conf.setDebug(true);
        
    // Uncomment when we want to submit to the actual cluster 
        
        //StormSubmitter.submitTopology("mqtt-event-processor",conf, builder.createTopology());
        
    //Or run locally with a timeout
        
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

        
        String configFileLocation = "mqtt_event_topology.properties";
        TwitterTestTopology mqttTopology 
                = new TwitterTestTopology(configFileLocation);
        mqttTopology.buildAndSubmit();
        

    }

    
}

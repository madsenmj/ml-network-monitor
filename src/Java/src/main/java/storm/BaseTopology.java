package storm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public abstract class BaseTopology {
	protected Properties topologyConfig;
	
	public BaseTopology(String configFileLocation) throws Exception {
		
		topologyConfig = new Properties();
		try {
		    System.out.println("Reading config from " + configFileLocation);
			topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
			//topologyConfig.load(configFileLocation);
		    System.out.println("Done reading config from " + configFileLocation);
			
		} catch (FileNotFoundException e) {
			System.out.println("Encountered FileNotFoundException while reading configuration properties: "
					+ e.getMessage());
			throw e;
		} catch (IOException e) {
			System.out.println("Encountered IOException while reading configuration properties: "
					+ e.getMessage());
			throw e;
		}			
	}
}

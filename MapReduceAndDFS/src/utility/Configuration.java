package utility;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/*
 * This class offers the configuration parameters loaded from the configuration file.
 */
public class Configuration extends ConfigurationBase implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 988346665358020534L;
	
	
	public Configuration(){
	    prop = new Properties();
        try {
            input = new FileInputStream("../masterConfig.properties");
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        try {
            prop.load(input);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	}
	
	
	
	public int getJobSubmission_port() {
		return Integer.valueOf(prop.getProperty("JobSubmissionPort"));
	}
	
	public int getHireWorkerServer_port() {
	    return Integer.valueOf(prop.getProperty("WorkerServerPort"));
	}
	
	
	public  Properties getProperties(){
	    return prop;
	}


    public int getNameNodeServer_port() {
        return Integer.valueOf(prop.getProperty("NameNodePort"));
    }
    
    public int getHireDataNodeServer_port() {
        return Integer.valueOf(prop.getProperty("DataNodeServerPort"));
    }

}

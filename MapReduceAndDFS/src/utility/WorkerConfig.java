package utility;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class WorkerConfig extends ConfigurationBase{
    
    public WorkerConfig(){
        
        prop = new Properties();
        try {
            input = new FileInputStream("../workerConfig.properties");
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
    
    public String getMasterAdd(){
        
        return prop.getProperty("MasterAddress");
        
    }
    public String getMasterPort(){
        
        return prop.getProperty("MasterPort");
    }
    
    

    @Override
    public Properties getProperties() {
        return prop;
    }

    public String getLocalPort() {
        return prop.getProperty("LocalPort");
    }
    
    public String getDataNodeServerPort() {
        return prop.getProperty("DataNodeServerPort");
    }
    
}
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
        System.out.println("master add"+prop.getProperty("MasterAddress"));
        return prop.getProperty("MasterAddress");
        
    }
    public String getMasterPort(){
        System.out.println("master port"+prop.getProperty("MasterPort"));
        return prop.getProperty("MasterPort");
    }
    
    

    @Override
    public Properties getProperties() {
        return prop;
    }
    
}